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
import cats.{Applicative, Foldable}
import cats.data.NonEmptyList
import cats.effect.{Async, Sync}
import cats.effect.kernel.Unique
import fs2.{Pipe, Stream}
import io.circe.syntax._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.{PolicyDecision, RetryDetails, RetryPolicy}
import retry.implicits._
import com.google.cloud.bigquery.FieldList

import scala.concurrent.duration.DurationLong
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.Duration
import com.snowplowanalytics.iglu.schemaddl.parquet.{Caster, Field}
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, ParsingError}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload => BadPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, ListOfList}
import com.snowplowanalytics.snowplow.streams.compression.Decompression._
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.runtime.Retrying.showRetryDetails
import com.snowplowanalytics.snowplow.loaders.transform.{BadRowsSerializer, NonAtomicFields, SchemaSubVersion, TabledEntity, Transform}
import com.snowplowanalytics.snowplow.bigquery.{Environment, RuntimeService}
import com.snowplowanalytics.snowplow.loaders.transform.NonAtomicFields.Result

import java.time.Instant

object Processing {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    implicit val lookup: RegistryLookup[F] = Http4sRegistryLookup(env.httpClient)
    val eventProcessingConfig              = EventProcessingConfig(EventProcessingConfig.NoWindowing, env.metrics.setLatency)
    val badProcessor                       = BadRowProcessor(env.appInfo.name, env.appInfo.version)
    Stream.eval(env.tableManager.createTableIfNotExists) *>
      Stream.eval(env.writer.opened.use_) *>
      env.source.decompressedStream(
        eventProcessingConfig,
        env.decompression,
        eventProcessor(env, badProcessor),
        badProcessor,
        toBadRow(badProcessor)
      )
  }

  /** Model used between stages of the processing pipeline */

  private case class ParseResult(
    events: List[Event],
    parseFailures: List[BadRow],
    countBytes: Long,
    token: Option[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  private case class Batched(
    events: ListOfList[Event],
    parseFailures: ListOfList[BadRow],
    countBytes: Long,
    entities: Map[TabledEntity, Set[SchemaSubVersion]],
    tokens: Vector[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  private type EventWithTransform = (Event, Map[String, AnyRef])

  /**
   * State of a batch for all stages post-transform
   *
   * @param toBeInserted
   *   Events from this batch which have not yet been inserted. Events are dropped from this list
   *   once they have either failed or got inserted. Implemented as a Vector because we need to do
   *   lookup by index.
   * @param v2Entities
   *   The typed Fields for self-describing entities in this batch.
   * @param origBatchSize
   *   The count of events in the original batch. Includes all good and bad events.
   * @param origBatchBytes
   *   The total size in bytes of events in the original batch. Includes all good and bad events.
   * @param badAccumulated
   *   Events that failed for any reason so far.
   * @param tokens
   *   The tokens to be emitted after we have finished processing all events
   */
  private case class BatchAfterTransform(
    toBeInserted: List[EventWithTransform],
    entities: Vector[Field],
    origBatchBytes: Long,
    origBatchCount: Long,
    badAccumulated: ListOfList[BadRow],
    tokens: Vector[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
  )

  private def eventProcessor[F[_]: Async: RegistryLookup](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): DecompressedEventProcessor[F] =
    _.through(parseBytes(badProcessor, env.cpuParallelism.parseBytes))
      .through(BatchUp.withTimeout(env.batching.maxBytes, env.batching.maxDelay))
      .through(transform(env, badProcessor, env.cpuParallelism.transform))
      .through(handleSchemaEvolution(env))
      .through(writeToBigQuery(env, badProcessor))
      .through(setE2ELatencyMetric(env))
      .through(sendFailedEvents(env, badProcessor))
      .through(sendMetrics(env))
      .through(emitTokens)

  private def toBadRow(processor: BadRowProcessor): DecompressionError => BadRow.LoaderParsingError =
    err =>
      BadRow.LoaderParsingError(
        processor,
        ParsingError.RowDecodingError(
          NonEmptyList.of(ParsingError.RowDecodingErrorInfo.UnhandledRowDecodingError(err.message))
        ),
        BadRowRawPayload(err.payload)
      )

  private def setE2ELatencyMetric[F[_]: Sync](env: Environment[F]): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap {
      _.earliestCollectorTstamp match {
        case Some(t) =>
          for {
            now <- Sync[F].realTime
            e2eLatency = now - t.toEpochMilli.millis
            _ <- env.metrics.setE2ELatency(e2eLatency)
          } yield ()
        case None => Sync[F].unit
      }
    }

  /** Parse raw bytes into Event using analytics sdk */
  private def parseBytes[F[_]: Async](
    badProcessor: BadRowProcessor,
    parallelism: Int
  ): Pipe[F, DecompressedTokenedEvents, ParseResult] =
    _.parEvalMap(parallelism) { result =>
      val payloads = result.payloads
      for {
        numBytes <- Sync[F].delay(payloads.foldLeft(0L)(_ + _.remaining().toLong))
        (badRows, events) <- Foldable[List].traverseSeparateUnordered(payloads) { byteBuffer =>
                               Sync[F].delay {
                                 Event.parseBytes(byteBuffer).toEither.leftMap { failure =>
                                   val payload = BadRowRawPayload(StandardCharsets.UTF_8.decode(byteBuffer).toString)
                                   BadRow.LoaderParsingError(badProcessor, failure, payload)
                                 }
                               }
                             }
        earliestCollectorTstamp = events.view.map(_.collector_tstamp).minOption
      } yield ParseResult(
        events,
        badRows ::: result.bad,
        numBytes,
        result.ack,
        earliestCollectorTstamp
      )
    }

  /** Transform the Event into values compatible with the BigQuery sdk */
  private def transform[F[_]: Async: RegistryLookup](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    parallelism: Int
  ): Pipe[F, Batched, BatchAfterTransform] =
    _.parEvalMap(parallelism) { case Batched(events, parseFailures, countBytes, entities, tokens, earliestCollectorTstamp) =>
      for {
        now <- Sync[F].realTimeInstant
        loadTstamp = BigQueryCaster.timestampValue(now)
        _ <- Logger[F].debug(s"Processing batch of size ${events.size}")
        v2NonAtomicFields <- resolveV2NonAtomicFields(env, entities)
        legacyFields <- LegacyColumns.resolveTypes[F](env.resolver, entities, env.legacyColumns, env.legacyColumnMode)
        _ <- possiblyExitOnMissingIgluSchema(env, v2NonAtomicFields, legacyFields)
        (moreBad, rows) <- transformBatch[F](badProcessor, loadTstamp, events, v2NonAtomicFields, legacyFields)
        fields = v2NonAtomicFields.fields.flatMap { tte =>
                   tte.mergedField :: tte.recoveries.map(_._2)
                 } ++ legacyFields.fields.map(f => LegacyColumns.dropJsonTypes(f.field))
      } yield BatchAfterTransform(
        rows,
        fields,
        countBytes,
        events.size + parseFailures.size,
        parseFailures.prepend(moreBad),
        tokens,
        earliestCollectorTstamp
      )
    }

  private def resolveV2NonAtomicFields[F[_]: Async: RegistryLookup](
    env: Environment[F],
    entities: Map[TabledEntity, Set[SchemaSubVersion]]
  ): F[Result] =
    if (env.legacyColumnMode) Sync[F].delay(Result(Vector.empty, Nil))
    else
      NonAtomicFields
        .resolveTypes[F](env.resolver, entities, env.schemasToSkip ::: env.legacyColumns)
        .flatMap(possiblyExitOnClashingIgluSchemas(env, _))

  private def transformBatch[F[_]: Sync](
    badProcessor: BadRowProcessor,
    loadTstamp: java.lang.Long,
    events: ListOfList[Event],
    v2Entities: NonAtomicFields.Result,
    legacyEntities: LegacyColumns.Result
  ): F[(List[BadRow], List[EventWithTransform])] =
    Foldable[ListOfList]
      .traverseSeparateUnordered(events) { event =>
        Sync[F].delay {
          val v2Transform = Transform
            .transformEvent[AnyRef](badProcessor, BigQueryCaster, event, v2Entities)
            .map { namedValues =>
              namedValues
                .map { case Caster.NamedValue(k, v) =>
                  k -> v
                }
                .toMap
                .updated("load_tstamp", loadTstamp)
            }
          val legacyTransform = LegacyColumns
            .transformEvent(badProcessor, event, legacyEntities)

          for {
            map1 <- v2Transform
            map2 <- legacyTransform
          } yield event -> (map1 ++ map2)
        }
      }

  private def writeToBigQuery[F[_]: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.parEvalMap(env.batching.writeBatchConcurrency) { batch =>
      writeUntilSuccessful(env, badProcessor, batch)
        .onError { _ =>
          env.appHealth.beUnhealthyForRuntimeService(RuntimeService.BigQueryClient)
        }
    }

  implicit private class WriteResultOps[F[_]: Async](val attempt: F[Writer.WriteResult]) {

    /**
     * Hard-coded constant. Forgive me.
     *
     * If we recover before reaching this number of retries, then there's really no need to log a
     * known exception. If we exceed this number of retries then it's unexpected, and it would be
     * helpful to have something in the logs instead of hiding the exception.
     */
    private def errorsAllowedWithShortLogging = 4

    def handlingServerSideSchemaMismatches(env: Environment[F]): F[Writer.WriteResult] = {
      def onFailure(wr: Writer.WriteResult, details: RetryDetails): F[Unit] = {
        val msg = show"Newly added columns have not yet propagated to the BigQuery Writer server-side. $details"
        val log = wr match {
          case Writer.WriteResult.ServerSideSchemaMismatch(e) if details.retriesSoFar > errorsAllowedWithShortLogging =>
            Logger[F].warn(e)(msg)
          case _ =>
            Logger[F].warn(msg)
        }
        log *> env.writer.closed.use_
      }
      attempt
        .retryingOnFailures(
          policy = env.alterTableWaitPolicy,
          wasSuccessful = {
            case Writer.WriteResult.ServerSideSchemaMismatch(_) => false.pure[F]
            case _                                              => true.pure[F]
          },
          onFailure = onFailure
        )
    }

    def handlingWriterWasClosedByEarlierErrors(env: Environment[F]): F[Writer.WriteResult] = {
      def onFailure(wr: Writer.WriteResult, details: RetryDetails): F[Unit] = {
        val msg =
          "BigQuery Writer was already closed by an earlier exception in a different Fiber.  Will reset the Writer and try again immediately"
        val log = wr match {
          case Writer.WriteResult.WriterWasClosedByEarlierError(e) if details.retriesSoFar > errorsAllowedWithShortLogging =>
            Logger[F].warn(e)(msg)
          case _ =>
            Logger[F].warn(msg)
        }
        log *> env.writer.closed.use_
      }
      val retryImmediately = PolicyDecision.DelayAndRetry(Duration.Zero)
      attempt
        .retryingOnFailures(
          policy = RetryPolicy.lift[F](_ => retryImmediately),
          wasSuccessful = {
            case Writer.WriteResult.WriterWasClosedByEarlierError(_) => false.pure[F]
            case _                                                   => true.pure[F]
          },
          onFailure = onFailure
        )
    }
  }

  private def writeUntilSuccessful[F[_]: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    if (batch.toBeInserted.isEmpty)
      batch.pure[F]
    else
      env.writer.opened
        .use(_.write(batch.toBeInserted.map(_._2)))
        .handlingWriterWasClosedByEarlierErrors(env)
        .handlingServerSideSchemaMismatches(env)
        .flatMap {
          case Writer.WriteResult.SerializationFailures(failures) =>
            val (badRows, tryAgains) = batch.toBeInserted.zipWithIndex.foldLeft((List.empty[BadRow], List.empty[EventWithTransform])) {
              case ((badRows, tryAgains), (eventWithTransform, index)) =>
                failures.get(index) match {
                  case Some(cause) =>
                    val badRow = BadRow.LoaderRuntimeError(badProcessor, cause, BadPayload.LoaderPayload(eventWithTransform._1))
                    (badRow :: badRows, tryAgains)
                  case None =>
                    (badRows, eventWithTransform :: tryAgains)
                }
            }
            writeUntilSuccessful(
              env,
              badProcessor,
              batch.copy(toBeInserted = tryAgains, badAccumulated = batch.badAccumulated.prepend(badRows))
            )
          case _ =>
            Sync[F].pure(batch.copy(toBeInserted = List.empty))
        }

  /**
   * Alters the table to add any columns that were present in the Events but not currently in the
   * table
   */
  private def handleSchemaEvolution[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      env.writer.opened
        .use(_.descriptor)
        .flatMap { descriptor =>
          val fieldsToAdd = BigQuerySchemaUtils.alterTableRequired(descriptor, batch.entities)
          if (fieldsToAdd.nonEmpty) {
            env.tableManager.addColumns(fieldsToAdd.toVector).flatMap { fieldsToExist =>
              openWriterUntilFieldsExist(env, fieldsToExist)
            }
          } else {
            Sync[F].unit
          }
        }
        .onError { _ =>
          env.appHealth.beUnhealthyForRuntimeService(RuntimeService.BigQueryClient)
        }
    }

  private def openWriterUntilFieldsExist[F[_]: Async](env: Environment[F], fieldsToExist: FieldList): F[Unit] =
    env.writer.opened
      .use(_.descriptor)
      .retryingOnFailures(
        policy = env.alterTableWaitPolicy,
        wasSuccessful = { descriptor =>
          (!BigQuerySchemaUtils.fieldsMissingFromDescriptor(descriptor, fieldsToExist)).pure[F]
        },
        onFailure = { case (_, details) =>
          val msg = show"Newly added columns have not yet propagated to the BigQuery Writer client-side. $details"
          Logger[F].warn(msg) *> env.writer.closed.use_
        }
      )
      .void

  private def sendFailedEvents[F[_]: Sync](
    env: Environment[F],
    badRowProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      if (batch.badAccumulated.nonEmpty) {
        val serialized =
          batch.badAccumulated.mapUnordered(badRow => BadRowsSerializer.withMaxSize(badRow, badRowProcessor, env.badRowMaxSize))
        env.badSink
          .sinkSimple(serialized)
          .onError { _ =>
            env.appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)
          }
      } else Applicative[F].unit
    }

  private def sendMetrics[F[_]: Applicative](env: Environment[F]): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      env.metrics.addGood(batch.origBatchCount - batch.badAccumulated.size) *> env.metrics.addBad(batch.badAccumulated.size)
    }

  private def emitTokens[F[_]]: Pipe[F, BatchAfterTransform, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private implicit def batchable: BatchUp.Batchable[ParseResult, Batched] = new BatchUp.Batchable[ParseResult, Batched] {
    def combine(b: Batched, a: ParseResult): Batched =
      Batched(
        events        = b.events.prepend(a.events),
        parseFailures = b.parseFailures.prepend(a.parseFailures),
        countBytes    = b.countBytes + a.countBytes,
        entities      = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_)) |+| b.entities,
        tokens        = b.tokens :++ a.token,
        chooseEarliestTstamp(a.earliestCollectorTstamp, b.earliestCollectorTstamp)
      )
    def single(a: ParseResult): Batched = {
      val entities = Foldable[List].foldMap(a.events)(TabledEntity.forEvent(_))
      Batched(
        ListOfList.of(List(a.events)),
        ListOfList.of(List(a.parseFailures)),
        a.countBytes,
        entities,
        a.token.toVector,
        a.earliestCollectorTstamp
      )
    }
    def weightOf(a: ParseResult): Long =
      a.countBytes
  }

  private def possiblyExitOnMissingIgluSchema[F[_]: Sync](
    env: Environment[F],
    v2NonAtomicFields: NonAtomicFields.Result,
    legacyFields: LegacyColumns.Result
  ): F[Unit] =
    if (env.exitOnMissingIgluSchema && (v2NonAtomicFields.igluFailures.nonEmpty || legacyFields.igluFailures.nonEmpty)) {
      val base =
        "Exiting because failed to resolve Iglu schemas.  Either check the configuration of the Iglu repos, or set the `skipSchemas` config option, or set `exitOnMissingIgluSchema` to false.\n"
      val failures = v2NonAtomicFields.igluFailures.map(_.failure) ::: legacyFields.igluFailures.map(_.failure)
      val msg      = failures.map(_.asJson.noSpaces).mkString(base, "\n", "")
      env.appHealth.beUnhealthyForRuntimeService(RuntimeService.Iglu) *> Logger[F].error(base) *> Sync[F].raiseError(
        new RuntimeException(msg)
      )
    } else Applicative[F].unit

  private def chooseEarliestTstamp(o1: Option[Instant], o2: Option[Instant]): Option[Instant] =
    (o1, o2)
      .mapN { case (t1, t2) =>
        if (t1.isBefore(t2)) t1 else t2
      }
      .orElse(o1)
      .orElse(o2)

  private def possiblyExitOnClashingIgluSchemas[F[_]: Sync](
    env: Environment[F],
    resolveTypesResult: Either[NonAtomicFields.ResolveTypesException, NonAtomicFields.Result]
  ): F[NonAtomicFields.Result] =
    resolveTypesResult match {
      case Left(e) =>
        Logger[F].error(e.getMessage) *> env.appHealth.beUnhealthyForRuntimeService(RuntimeService.Iglu) *> Sync[F].raiseError(e)
      case Right(r) => Applicative[F].pure(r)
    }

}
