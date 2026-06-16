/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.implicits._
import cats.effect.{Async, Poll, Resource, Sync}
import com.google.api.gax.batching.FlowControlSettings
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.json.JSONArray
import com.google.protobuf.Descriptors
import com.google.cloud.bigquery.storage.v1.{
  BigQueryWriteClient,
  BigQueryWriteSettings,
  Exceptions => BQExceptions,
  GetWriteStreamRequest,
  JsonStreamWriter,
  TableSchema,
  WriteStreamView
}
import com.google.cloud.bigquery.storage.v1.stub.BigQueryWriteStubSettings
import com.google.api.gax.core.CredentialsProvider
import com.google.auth.Credentials
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.api.gax.rpc.FixedHeaderProvider
import com.snowplowanalytics.snowplow.bigquery.{Alert, Config, RuntimeService}
import com.snowplowanalytics.snowplow.runtime.AppHealth
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap

import scala.jdk.CollectionConverters._

trait Writer[F[_]] {

  def descriptor: F[Descriptors.Descriptor]

  /**
   * Writes rows to BigQuery
   *
   * @param rows
   *   The rows to be inserted
   * @return
   *   List of the details of any insert failures. Empty list implies complete success.
   */
  def write(rows: List[Map[String, AnyRef]]): F[Writer.WriteResult]
}

object Writer {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  trait CloseableWriter[F[_]] extends Writer[F] {

    /** Closes the BigQuery Writer */
    def close: F[Unit]
  }

  /**
   * Provider of open BigQuery Writers
   *
   * The Builder is not responsible for closing any opened writer. Writer lifecycle must be managed
   * by the surrounding application code.
   */
  trait Builder[F[_]] {
    def build: F[CloseableWriter[F]]
  }

  type Provider[F[_]] = Coldswap[F, Writer[F]]

  sealed trait WriteResult

  object WriteResult {

    case object Success extends WriteResult

    /**
     * The result of `write` when some events could not be serialized according to the underlying
     * protobuf descriptor
     *
     * This is a **client-side** failure. The schema-aware Writer rejects the events before sending
     * them to BigQuery.
     *
     * This indicates a mis-match in schema between the incoming events and the target table.
     *
     * @param failuresByIndex
     *   A map of failure messages, keyed by the index of the event in the original list of rows. If
     *   an event is not listed in this map, it has **not** been written to BigQuery, but we can
     *   assume that event has no serialization problems.
     */
    case class SerializationFailures(failuresByIndex: Map[Int, String]) extends WriteResult

    /**
     * The result of `write` when the events are rejected by BigQuery because the events do not
     * match the table's schema
     *
     * This is a **server-side** failure. The schema-aware Writer (client) accepted the events, but
     * they got rejected after sending them to BigQuery.
     *
     * This exception is expected in the short period immediately after altering a table to add new
     * columns.
     */
    case class ServerSideSchemaMismatch(cause: Exception) extends WriteResult

    /**
     * The result of `write` when the underlying Writer was already closed for some other reason.
     *
     * This could happen if we have multiple fibers calling `write` in parallel, and if an exception
     * happened in one of the other fibers. A fiber receiving this exception should close the writer
     * resource and try again.
     */
    case class WriterWasClosedByEarlierError(cause: Exception) extends WriteResult
  }

  def builder[F[_]: Async](
    config: Config.BigQuery,
    credentials: Credentials
  ): Resource[F, Builder[F]] =
    for {
      client <- createWriteClient(credentials)
    } yield new Builder[F] {
      def build: F[CloseableWriter[F]] =
        buildWriter[F](config, client)
          .map(impl[F])
          .flatTap { w =>
            for {
              descriptor <- w.descriptor
              _ <- Logger[F].info(s"Opened Writer with fields: ${BigQuerySchemaUtils.showDescriptor(descriptor)}")
            } yield ()
          }
    }

  def provider[F[_]: Async](
    builder: Builder[F],
    retries: Config.Retries,
    health: AppHealth.Interface[F, Alert, RuntimeService]
  ): Resource[F, Provider[F]] =
    Coldswap.make(builderToResource(builder, retries, health))

  private def builderToResource[F[_]: Async](
    builder: Builder[F],
    retries: Config.Retries,
    health: AppHealth.Interface[F, Alert, RuntimeService]
  ): Resource[F, Writer[F]] = {
    def make(poll: Poll[F]) = poll {
      BigQueryRetrying.withRetries(health, retries, Alert.FailedToOpenBigQueryWriter(_)) {
        builder.build <* health.beHealthyForSetup
      }
    }

    Resource.makeFull(make)(_.close)
  }

  private def impl[F[_]: Async](writer: JsonStreamWriter): CloseableWriter[F] =
    new CloseableWriter[F] {

      def descriptor: F[Descriptors.Descriptor] =
        Sync[F].delay(writer.getDescriptor)

      def write(rows: List[Map[String, AnyRef]]): F[WriteResult] =
        Sync[F]
          .delay(writer.append(new JSONArray(rows.map(_.asJava).asJava)))
          .flatMap(FutureInterop.fromFuture(_))
          .as[WriteResult](WriteResult.Success)
          .recover {
            // Some of these errors can happen either client side during `writer.append`
            // ...or server-side during `FutureInterop.fromFuture(_)`
            case e: BQExceptions.SchemaMismatchedException =>
              WriteResult.ServerSideSchemaMismatch(e)
            case e: BQExceptions.StreamWriterClosedException =>
              WriteResult.WriterWasClosedByEarlierError(e)
            case e: BQExceptions.AppendSerializationError =>
              WriteResult.SerializationFailures {
                e.getRowIndexToErrorMessage.asScala.map { case (i, cause) =>
                  i.toInt -> cause
                }.toMap
              }
          }

      def close: F[Unit] =
        Logger[F].info("Closing BigQuery stream writer...") *>
          Sync[F].blocking(writer.close())
    }

  private def buildWriter[F[_]: Async](
    config: Config.BigQuery,
    client: BigQueryWriteClient
  ): F[JsonStreamWriter] = {
    val streamId = BigQueryUtils.streamIdOf(config)
    for {
      _ <- Logger[F].info(show"Opening BigQuery stream writer: $streamId")
      // Set the table schema explicitly, to prevent the JsonStreamWriter from fetching it automatically when we're not in control.
      schema <- getTableSchema[F](client, config)
      w <- Sync[F].blocking {
             JsonStreamWriter
               .newBuilder(streamId, schema, client)
               .setFlowControlSettings {
                 FlowControlSettings
                   .newBuilder()
                   // Large values so we never block underlying writer threads
                   .setMaxOutstandingElementCount(1_000_000L) // Default 1000
                   .setMaxOutstandingRequestBytes(1024 * 1024 * 1024) // 1Gb. Default 100 Mb.
                   .build
               }
               .build
           }
    } yield w
  }

  private def createWriteClient[F[_]: Sync](
    credentials: Credentials
  ): Resource[F, BigQueryWriteClient] = {
    val make = Sync[F].delay {
      val settingsBuilder = BigQueryWriteSettings.newBuilder
        .setHeaderProvider(FixedHeaderProvider.create("user-agent", BigQueryUtils.gcpUserAgent))
      settingsBuilder.getStubSettingsBuilder.setCredentialsProvider(credentialsProvider(credentials))
      BigQueryWriteClient.create(settingsBuilder.build)
    }
    Resource.make(make)(c => Sync[F].blocking(c.close()))
  }

  private def credentialsProvider(credentials: Credentials): CredentialsProvider =
    new CredentialsProvider {
      def getCredentials(): Credentials = credentials match {
        case sac: ServiceAccountCredentials =>
          if (sac.createScopedRequired)
            // TODO: Is this needed?
            sac.createScoped(BigQueryWriteStubSettings.getDefaultServiceScopes) match {
              case sac: ServiceAccountCredentials =>
                sac.createWithUseJwtAccessWithScope(true)
              case other => other
            }
          else
            sac.createWithUseJwtAccessWithScope(true)
        case other => other
      }
    }

  private def getTableSchema[F[_]: Sync](client: BigQueryWriteClient, config: Config.BigQuery): F[TableSchema] = {
    val streamId = BigQueryUtils.streamIdOf(config)
    val request  = GetWriteStreamRequest.newBuilder.setName(streamId).setView(WriteStreamView.FULL).build

    Sync[F]
      .blocking(client.getWriteStream(request))
      .map(_.getTableSchema)
  }

}
