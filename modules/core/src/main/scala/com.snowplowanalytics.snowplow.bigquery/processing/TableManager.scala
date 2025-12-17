/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.Show
import cats.implicits._
import cats.effect.implicits._
import cats.effect.{Async, Ref, Sync}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.google.cloud.bigquery.{
  BigQuery,
  BigQueryOptions,
  Clustering,
  FieldList,
  Schema,
  StandardTableDefinition,
  Table,
  TableDefinition,
  TableInfo,
  TimePartitioning
}
import com.google.auth.Credentials
import com.google.cloud.bigquery.BigQueryException
import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.runtime.{AppHealth, SetupExceptionMessages}
import com.snowplowanalytics.snowplow.loaders.transform.AtomicFields
import com.snowplowanalytics.snowplow.bigquery.{Alert, Config, RuntimeService}
import com.snowplowanalytics.snowplow.bigquery.processing.BigQueryUtils.BQExceptionSyntax

import scala.jdk.CollectionConverters._

trait TableManager[F[_]] {

  /**
   * Attempt to add columns to the table
   *
   * @return
   *   A list of fields which are guaranteed to eventually exist in the table. Fields might not be
   *   available immediately due to asynchronous nature of BigQuery. The returned list will be empty
   *   if adding columns failed due to too many columns in the table.
   */
  def addColumns(columns: Vector[Field]): F[FieldList]

  def tableExists: F[Boolean]

  def createTable: F[Unit]

}

object TableManager {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  trait WithHandledErrors[F[_]] {
    def addColumns(columns: Vector[Field]): F[FieldList]
    def createTableIfNotExists: F[Unit]
  }

  def make[F[_]: Async](
    config: Config.BigQuery,
    credentials: Credentials
  ): F[TableManager[F]] =
    for {
      client <- Sync[F].delay(BigQueryOptions.newBuilder.setCredentials(credentials).build.getService)
    } yield impl(config, client)

  def withHandledErrors[F[_]: Async](
    underlying: TableManager[F],
    retries: Config.Retries,
    appHealth: AppHealth.Interface[F, Alert, RuntimeService]
  ): F[WithHandledErrors[F]] =
    for {
      addingColumnsEnabled <- Ref[F].of[Boolean](true)
    } yield new WithHandledErrors[F] {
      def addColumns(columns: Vector[Field]): F[FieldList] =
        addingColumnsEnabled.get.flatMap {
          case true =>
            BigQueryRetrying
              .withRetries(appHealth, retries, Alert.FailedToAddColumns(columns.map(_.name), _)) {
                underlying
                  .addColumns(columns)
                  .recoverWith(handleTooManyColumns(retries, appHealth, addingColumnsEnabled, columns))
                  .onError(logOnRaceCondition)
              }
          case false =>
            FieldList.of().pure[F]
        }

      def createTableIfNotExists: F[Unit] =
        BigQueryRetrying
          .withRetries(appHealth, retries, Alert.FailedToGetTable(_)) {
            underlying.tableExists
              .recoverWith {
                case bqe: BigQueryException if bqe.lowerCaseReason === "accessdenied" =>
                  Logger[F].info(bqe)("Failed to get details of existing table.  Will fallback to creating the table...").as(false)
              }
          }
          .flatMap {
            case true =>
              Sync[F].unit
            case false =>
              BigQueryRetrying.withRetries(appHealth, retries, Alert.FailedToCreateEventsTable(_)) {
                underlying.createTable
                  .recoverWith {
                    case bqe: BigQueryException if bqe.lowerCaseReason === "duplicate" =>
                      // Table already exists
                      Logger[F].info(s"Ignoring error when creating table: ${bqe.getMessage}")
                  }
              }
          }
    }

  private def impl[F[_]: Async](
    config: Config.BigQuery,
    client: BigQuery
  ): TableManager[F] = new TableManager[F] {

    def addColumns(columns: Vector[Field]): F[FieldList] =
      for {
        _ <- Logger[F].info(s"Attempting to fetch details of table ${config.dataset}.${config.table}...")
        table <- Sync[F].blocking(client.getTable(BigQueryUtils.tableIdOf(config)))
        _ <- Logger[F].info("Successfully fetched details of table")
        schema <- Sync[F].pure(table.getDefinition[TableDefinition].getSchema)
        fields <- Sync[F].pure(schema.getFields)
        fields <- Sync[F].pure(BigQuerySchemaUtils.mergeInColumns(fields, columns))
        schema <- Sync[F].pure(Schema.of(fields))
        table <- Sync[F].pure(setTableSchema(table, schema))
        _ <- Logger[F].info(s"Altering table to add columns [${showColumns(columns)}]...")
        _ <- Sync[F].blocking(table.update())
        _ <- Logger[F].info("Successfully altered table schema")
      } yield fields

    def tableExists: F[Boolean] =
      for {
        _ <- Logger[F].info(s"Attempting to fetch details of table ${config.dataset}.${config.table}...")
        attempt <- Sync[F].blocking(Option(client.getTable(BigQueryUtils.tableIdOf(config))))
        result <- attempt match {
                    case Some(_) =>
                      Logger[F].info("Successfully fetched details of table").as(true)
                    case None =>
                      Logger[F].info("Tried to fetch details of existing table but it does not already exist").as(false)
                  }
      } yield result

    def createTable: F[Unit] = {
      val tableInfo = atomicTableInfo(config)
      Logger[F].info(show"Creating table $tableInfo") *>
        Sync[F].blocking(client.create(tableInfo)) *>
        Logger[F].info(show"Successfully created table $tableInfo")
    }
  }

  private def setTableSchema(table: Table, schema: Schema): Table =
    table.toBuilder().setDefinition(StandardTableDefinition.of(schema)).build()

  private def logOnRaceCondition[F[_]: Sync]: PartialFunction[Throwable, F[Unit]] = {
    case bqe: BigQueryException if bqe.lowerCaseReason === "invalid" =>
      Logger[F].warn(s"Caught known exception which probably means another loader has already altered the table.")
    // Don't do anything else; the BigQueryRetrying will handle retries and logging the exception.
  }

  private def handleTooManyColumns[F[_]: Async](
    retries: Config.Retries,
    appHealth: AppHealth.Interface[F, Alert, ?],
    addingColumnsEnabled: Ref[F, Boolean],
    columns: Vector[Field]
  ): PartialFunction[Throwable, F[FieldList]] = {
    case bqe: BigQueryException
        if bqe.lowerCaseReason === "invalid" && (bqe.lowerCaseMessage
          .startsWith("too many columns") || bqe.lowerCaseMessage.startsWith("too many total leaf fields")) =>
      val enableAfterDelay = Async[F].sleep(retries.tooManyColumns.delay) *> addingColumnsEnabled.set(true)
      for {
        _ <- Logger[F].error(bqe)(s"Could not alter table schema because of too many columns")
        _ <- appHealth.beUnhealthyForSetup(
               Alert.FailedToAddColumns(columns.map(_.name), SetupExceptionMessages(List(bqe.getMessage)))
             )
        _ <- addingColumnsEnabled.set(false)
        _ <- enableAfterDelay.start
      } yield FieldList.of()
  }

  private def showColumns(columns: Vector[Field]): String =
    columns.map(_.name).mkString(", ")

  private def atomicTableInfo(config: Config.BigQuery): TableInfo = {
    val atomicFields = AtomicFields.withLoadTstamp.map(BigQuerySchemaUtils.bqFieldOf)
    val fields       = FieldList.of(atomicFields.asJava)
    val schema       = Schema.of(fields)
    val tableDefinition = StandardTableDefinition.newBuilder
      .setSchema(schema)
      .setTimePartitioning {
        TimePartitioning
          .newBuilder(TimePartitioning.Type.DAY)
          .setField("load_tstamp")
          .build()
      }
      .setClustering {
        Clustering
          .newBuilder()
          .setFields(List("event_name").asJava)
          .build()
      }
      .build()
    TableInfo.of(BigQueryUtils.tableIdOf(config), tableDefinition)
  }

  private implicit val showTableInfo: Show[TableInfo] = Show(_.getTableId.getIAMResourceName)

}
