/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.implicits._
import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import org.http4s.client.Client
import fs2.Stream
import com.google.protobuf.Descriptors
import com.google.cloud.bigquery.FieldList

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.iglu.schemaddl.parquet.Field
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, Retrying}
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap
import com.snowplowanalytics.snowplow.streams.{
  EventProcessingConfig,
  EventProcessor,
  ListOfList,
  Sink,
  Sinkable,
  SourceAndAck,
  TokenedEvents
}
import com.snowplowanalytics.snowplow.bigquery.processing.{BigQueryRetrying, TableManager, Writer}
import com.snowplowanalytics.snowplow.bigquery.MockEnvironment.State

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class MockEnvironment(state: Ref[IO, State], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case object CreatedTable extends Action
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Long) extends Action
    case class AlterTableAddedColumns(columns: Vector[String]) extends Action
    case object OpenedWriter extends Action
    case object ClosedWriter extends Action
    case class WroteRowsToBigQuery(rowCount: Int) extends Action
    case class AddedGoodCountMetric(count: Long) extends Action
    case class AddedBadCountMetric(count: Long) extends Action
    case class SetLatencyMetric(latency: FiniteDuration) extends Action
    case class SetE2ELatencyMetric(e2eLatency: FiniteDuration) extends Action
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
  }
  import Action._

  case class State(actions: Vector[Action], writtenToBQ: Iterable[Map[String, AnyRef]])

  /**
   * Build a mock environment for testing
   *
   * @param inputs
   *   Input events to send into the environment.
   * @param mocks
   *   Responses we want the `Writer` to return when someone calls uses the mocked services
   * @param legacyColumns
   *   Schemas for which to use legacy column style of BigQuery Loader version 1
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(
    inputs: List[TokenedEvents],
    mocks: Mocks,
    legacyColumns: List[SchemaCriterion],
    legacyColumnMode: Boolean
  ): Resource[IO, MockEnvironment] =
    for {
      state <- Resource.eval(Ref[IO].of(State(Vector.empty[Action], Nil)))
      writerResource <- Resource.eval(testWriter(state, mocks.writerResponses, mocks.descriptors))
      writerColdswap <- Coldswap.make(writerResource)
    } yield {
      val env = Environment(
        appInfo              = appInfo,
        source               = testSourceAndAck(inputs, state),
        badSink              = testBadSink(mocks.badSinkResponse, state),
        resolver             = Resolver[IO](Nil, None),
        httpClient           = testHttpClient,
        tableManager         = testTableManager(mocks.addColumnsResponse, state),
        writer               = writerColdswap,
        metrics              = testMetrics(state),
        appHealth            = testAppHealth(state),
        alterTableWaitPolicy = BigQueryRetrying.policyForAlterTableWait[IO](retriesConfig),
        batching = Config.Batching(
          maxBytes              = 10000000,
          maxDelay              = 10.seconds,
          writeBatchConcurrency = 1
        ),
        cpuParallelism = CpuParallelism(
          parseBytes = 1,
          transform  = 1
        ),
        badRowMaxSize           = 1000000,
        schemasToSkip           = List.empty,
        legacyColumns           = legacyColumns,
        legacyColumnMode        = legacyColumnMode,
        exitOnMissingIgluSchema = false
      )
      MockEnvironment(state, env)
    }

  final case class Mocks(
    writerResponses: List[Response[Writer.WriteResult]],
    badSinkResponse: Response[Unit],
    addColumnsResponse: Response[FieldList],
    descriptors: List[Descriptors.Descriptor]
  )

  object Mocks {
    val default: Mocks = Mocks(
      writerResponses    = List.empty,
      badSinkResponse    = Response.Success(()),
      addColumnsResponse = Response.Success(FieldList.of()),
      descriptors        = List.empty
    )
  }

  sealed trait Response[+A]
  object Response {
    final case class Success[A](value: A) extends Response[A]
    final case class ExceptionThrown(value: Throwable) extends Response[Nothing]
  }

  val appInfo = new AppInfo {
    def name        = "bigquery-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/bigquery-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  private def testTableManager(mockedResponse: Response[FieldList], state: Ref[IO, State]): TableManager.WithHandledErrors[IO] =
    new TableManager.WithHandledErrors[IO] {
      def addColumns(columns: Vector[Field]): IO[FieldList] =
        mockedResponse match {
          case Response.Success(fieldList) =>
            state.update(s => s.copy(actions = s.actions :+ AlterTableAddedColumns(columns.map(_.name)))).as(fieldList)
          case Response.ExceptionThrown(value) =>
            IO.raiseError(value)
        }

      def createTableIfNotExists: IO[Unit] =
        state.update(s => s.copy(actions = s.actions :+ CreatedTable))
    }

  private def testSourceAndAck(inputs: List[TokenedEvents], state: Ref[IO, State]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream
          .emits(inputs)
          .through(processor)
          .chunks
          .evalMap { chunk =>
            state.update(s => s.copy(actions = s.actions :+ Checkpointed(chunk.toList)))
          }
          .drain

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] =
        IO.pure(None)
    }

  private def testBadSink(mockedResponse: Response[Unit], state: Ref[IO, State]): Sink[IO] = new Sink[IO] {
    override def sink(batch: ListOfList[Sinkable]): IO[Unit] =
      mockedResponse match {
        case Response.Success(_) =>
          state.update(s => s.copy(actions = s.actions :+ SentToBad(batch.size)))
        case Response.ExceptionThrown(value) =>
          IO.raiseError(value)
      }

    override def isHealthy: IO[Boolean] = IO.pure(true)
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  /**
   * Mocked implementation of a `Writer`
   *
   * @param actionRef
   *   Global Ref used to accumulate actions that happened
   * @param responses
   *   Responses that this mocked Writer should return each time someone calls `write`. If no
   *   responses given, then it will return with a successful response.
   */
  private def testWriter(
    stateRef: Ref[IO, State],
    responses: List[Response[Writer.WriteResult]],
    descriptors: List[Descriptors.Descriptor]
  ): IO[Resource[IO, Writer[IO]]] =
    for {
      responseRef <- Ref[IO].of(responses)
      descriptorRef <- Ref[IO].of(descriptors)
    } yield {
      val make = stateRef.update(s => s.copy(actions = s.actions :+ OpenedWriter)).as {
        new Writer[IO] {
          def descriptor: IO[Descriptors.Descriptor] =
            descriptorRef.modify {
              case head :: tail => (tail, head)
              case Nil          => (Nil, AtomicDescriptor.withWebPage(false))
            }

          def write(rows: List[Map[String, AnyRef]]): IO[Writer.WriteResult] =
            for {
              response <- responseRef.modify {
                            case head :: tail => (tail, head)
                            case Nil          => (Nil, Response.Success(Writer.WriteResult.Success))
                          }
              writeResult <- response match {
                               case success: Response.Success[Writer.WriteResult] =>
                                 updateActions(stateRef, rows, success) *> IO(success.value)
                               case Response.ExceptionThrown(ex) =>
                                 IO.raiseError(ex)
                             }
            } yield writeResult

          private def updateActions(
            state: Ref[IO, State],
            rows: Iterable[Map[String, AnyRef]],
            success: Response.Success[Writer.WriteResult]
          ): IO[Unit] =
            success.value match {
              case Writer.WriteResult.Success =>
                state.update(s => s.copy(actions = s.actions :+ WroteRowsToBigQuery(rows.size), writtenToBQ = s.writtenToBQ ++ rows))
              case _ =>
                IO.unit
            }
        }
      }
      Resource.make(make)(_ => stateRef.update(s => s.copy(actions = s.actions :+ ClosedWriter)))
    }

  private def testMetrics(ref: Ref[IO, State]): Metrics[IO] = new Metrics[IO] {
    def addBad(count: Long): IO[Unit] =
      ref.update(s => s.copy(actions = s.actions :+ AddedBadCountMetric(count)))

    def addGood(count: Long): IO[Unit] =
      ref.update(s => s.copy(actions = s.actions :+ AddedGoodCountMetric(count)))

    def setLatency(latency: FiniteDuration): IO[Unit] =
      ref.update(s => s.copy(actions = s.actions :+ SetLatencyMetric(latency)))

    def setE2ELatency(e2eLatency: FiniteDuration): IO[Unit] =
      ref.update(s => s.copy(actions = s.actions :+ SetE2ELatencyMetric(e2eLatency)))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }

  private def testAppHealth(ref: Ref[IO, State]): AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        IO.unit
      def beUnhealthyForSetup(alert: Alert): IO[Unit] =
        IO.unit
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(s => s.copy(actions = s.actions :+ BecameHealthy(service)))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(s => s.copy(actions = s.actions :+ BecameUnhealthy(service)))
    }

  private def retriesConfig = Config.Retries(
    Retrying.Config.ForSetup(30.seconds),
    Retrying.Config.ForTransient(1.second, 5),
    Config.AlterTableWaitRetries(1.second),
    Config.TooManyColumnsRetries(300.seconds)
  )
}
