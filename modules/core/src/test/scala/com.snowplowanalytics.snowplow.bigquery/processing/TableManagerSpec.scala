/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.effect.{IO, Ref}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import io.grpc.{Status => GrpcStatus, StatusRuntimeException}
import com.google.cloud.bigquery.{BigQueryError, BigQueryException, FieldList}
import com.google.api.client.http.{HttpHeaders, HttpResponseException}

import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.snowplow.bigquery.{Alert, Config, RuntimeService}
import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying}

class TableManagerSpec extends Specification with CatsEffect {
  import TableManagerSpec._

  def is = s2"""
  The table manager
    Add columns when told to $e1
    Retry adding columns and send alerts when there is a setup exception $e2
    Retry adding columns if there is a transient exception, with limited number of attempts and no monitoring alerts $e3
    Become healthy after recovering from an earlier 'permission denied' setup error $e4_1
    Become healthy after recovering from an earlier 'account not found' setup error $e4_2
    Become healthy after recovering from an earlier transient error $e5
    Disable future attempts to add columns if the table has too many columns - exception type 1 $e6_1
    Disable future attempts to add columns if the table has too many columns - exception type 2 $e6_2
    Eventually re-enable future attempts to add columns if the table has too many columns - exception type 1 $e7_1
    Eventually re-enable future attempts to add columns if the table has too many columns - exception type 2 $e7_2

    Attempt to get the existing table if told to create if not exists $create1
    Attempt to create the table if failed to get existing table $create2

  """

  def e1 = control().flatMap { c =>
    val testFields1 = Vector(
      Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
      Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
    )

    val testFields2 = Vector(
      Field("f3", Type.String, Type.Nullability.Nullable, Set("f3")),
      Field("f4", Type.Integer, Type.Nullability.Required, Set("f4"))
    )

    val io = for {
      tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
      _ <- tableManager.addColumns(testFields1)
      _ <- tableManager.addColumns(testFields2)
    } yield ()

    val expected = Vector(
      Action.AddColumnsAttempted(testFields1),
      Action.BecameHealthy(RuntimeService.BigQueryClient),
      Action.AddColumnsAttempted(testFields2),
      Action.BecameHealthy(RuntimeService.BigQueryClient)
    )

    for {
      _ <- io
      state <- c.state.get
    } yield state should beEqualTo(expected)
  }

  def e2 = {
    val mocks = List.fill(100)(Response.ExceptionThrown(testGrpcPermissionDeniedSetupError))

    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.addColumns(testFields)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthyForSetup,
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthyForSetup,
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthyForSetup,
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthyForSetup
      )

      val test = for {
        fiber <- io.start
        _ <- IO.sleep(4.minutes)
        _ <- fiber.cancel
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e3 = {
    val mocks = List.fill(100)(Response.ExceptionThrown(new RuntimeException("Boom!")))
    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.addColumns(testFields)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthy(RuntimeService.BigQueryClient),
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthy(RuntimeService.BigQueryClient),
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthy(RuntimeService.BigQueryClient),
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthy(RuntimeService.BigQueryClient),
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthy(RuntimeService.BigQueryClient)
      )

      val test = for {
        _ <- io.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e4_common(exception: Throwable) = {
    val mocks = List(Response.ExceptionThrown(exception))

    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.addColumns(testFields)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthyForSetup,
        Action.AddColumnsAttempted(testFields),
        Action.BecameHealthy(RuntimeService.BigQueryClient)
      )

      val test = for {
        _ <- io.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e4_1 = e4_common(testGrpcPermissionDeniedSetupError)

  def e4_2 = e4_common(testAccountNotFoundSetupError)

  def e5 = {
    val mocks = List(Response.ExceptionThrown(new RuntimeException("boom!")))

    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.addColumns(testFields)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields),
        Action.BecameUnhealthy(RuntimeService.BigQueryClient),
        Action.AddColumnsAttempted(testFields),
        Action.BecameHealthy(RuntimeService.BigQueryClient)
      )

      val test = for {
        _ <- io.voidError
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e6_1 = {
    val error     = new BigQueryError("invalid", "global", "Too many columns (10067) in schema. Only 10000 columns are allowed.")
    val exception = new BigQueryException(400, error.getMessage, error)
    val mocks     = List(Response.ExceptionThrown(exception))
    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields1 = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val testFields2 = Vector(
        Field("f3", Type.String, Type.Nullability.Nullable, Set("f3")),
        Field("f4", Type.Integer, Type.Nullability.Required, Set("f4"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.addColumns(testFields1)
        _ <- IO.sleep(10.seconds)
        _ <- tableManager.addColumns(testFields2)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields1),
        Action.BecameUnhealthyForSetup,
        Action.BecameHealthy(RuntimeService.BigQueryClient)
      )

      val test = for {
        _ <- io
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e6_2 = {
    val error     = new BigQueryError("invalid", "global", "Too many total leaf fields: 10001, max allowed field count: 10000")
    val exception = new BigQueryException(400, error.getMessage, error)
    val mocks     = List(Response.ExceptionThrown(exception))
    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields1 = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val testFields2 = Vector(
        Field("f3", Type.String, Type.Nullability.Nullable, Set("f3")),
        Field("f4", Type.Integer, Type.Nullability.Required, Set("f4"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.addColumns(testFields1)
        _ <- IO.sleep(10.seconds)
        _ <- tableManager.addColumns(testFields2)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields1),
        Action.BecameUnhealthyForSetup,
        Action.BecameHealthy(RuntimeService.BigQueryClient)
      )

      val test = for {
        _ <- io
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e7_1 = {
    val error     = new BigQueryError("invalid", "global", "Too many columns (10067) in schema. Only 10000 columns are allowed.")
    val exception = new BigQueryException(400, error.getMessage, error)
    val mocks     = List(Response.ExceptionThrown(exception))
    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields1 = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val testFields2 = Vector(
        Field("f3", Type.String, Type.Nullability.Nullable, Set("f3")),
        Field("f4", Type.Integer, Type.Nullability.Required, Set("f4"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.addColumns(testFields1)
        _ <- IO.sleep(310.seconds)
        _ <- tableManager.addColumns(testFields2)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields1),
        Action.BecameUnhealthyForSetup,
        Action.BecameHealthy(RuntimeService.BigQueryClient),
        Action.AddColumnsAttempted(testFields2),
        Action.BecameHealthy(RuntimeService.BigQueryClient)
      )

      val test = for {
        _ <- io
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def e7_2 = {
    val error     = new BigQueryError("invalid", "global", "Too many total leaf fields: 10001, max allowed field count: 10000")
    val exception = new BigQueryException(400, error.getMessage, error)
    val mocks     = List(Response.ExceptionThrown(exception))
    control(Mocks(addColumnsResults = mocks)).flatMap { c =>
      val testFields1 = Vector(
        Field("f1", Type.String, Type.Nullability.Nullable, Set("f1")),
        Field("f2", Type.Integer, Type.Nullability.Required, Set("f2"))
      )

      val testFields2 = Vector(
        Field("f3", Type.String, Type.Nullability.Nullable, Set("f3")),
        Field("f4", Type.Integer, Type.Nullability.Required, Set("f4"))
      )

      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.addColumns(testFields1)
        _ <- IO.sleep(310.seconds)
        _ <- tableManager.addColumns(testFields2)
      } yield ()

      val expected = Vector(
        Action.AddColumnsAttempted(testFields1),
        Action.BecameUnhealthyForSetup,
        Action.BecameHealthy(RuntimeService.BigQueryClient),
        Action.AddColumnsAttempted(testFields2),
        Action.BecameHealthy(RuntimeService.BigQueryClient)
      )

      val test = for {
        _ <- io
        state <- c.state.get
      } yield state should beEqualTo(expected)

      TestControl.executeEmbed(test)
    }
  }

  def create1 = control().flatMap { c =>
    val io = for {
      tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
      _ <- tableManager.createTableIfNotExists
    } yield ()

    val expected = Vector(
      Action.GetTableAttempted,
      Action.BecameHealthy(RuntimeService.BigQueryClient)
    )

    for {
      _ <- io
      state <- c.state.get
    } yield state should beEqualTo(expected)
  }

  def create2 = {
    val mocks = Mocks(getTableResults = List(Response.ExceptionThrown(testFailedToGetTableError)))

    control(mocks).flatMap { c =>
      val io = for {
        tableManager <- TableManager.withHandledErrors(c.tableManager, retriesConfig, c.appHealth)
        _ <- tableManager.createTableIfNotExists
      } yield ()

      val expected = Vector(
        Action.GetTableAttempted,
        Action.BecameHealthy(RuntimeService.BigQueryClient),
        Action.CreateTableAttempted,
        Action.BecameHealthy(RuntimeService.BigQueryClient)
      )

      for {
        _ <- io
        state <- c.state.get
      } yield state should beEqualTo(expected)
    }
  }

}

object TableManagerSpec {

  sealed trait Action

  object Action {
    case class AddColumnsAttempted(columns: Vector[Field]) extends Action
    case object CreateTableAttempted extends Action
    case object GetTableAttempted extends Action
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
    case object BecameHealthyForSetup extends Action
    case object BecameUnhealthyForSetup extends Action
  }

  sealed trait Response
  object Response {
    case object Success extends Response
    final case class ExceptionThrown(value: Throwable) extends Response
  }

  case class Mocks(addColumnsResults: List[Response] = Nil, getTableResults: List[Response] = Nil)

  case class Control(
    state: Ref[IO, Vector[Action]],
    tableManager: TableManager[IO],
    appHealth: AppHealth.Interface[IO, Alert, RuntimeService]
  )

  def retriesConfig = Config.Retries(
    Retrying.Config.ForSetup(30.seconds),
    Retrying.Config.ForTransient(1.second, 5),
    Config.AlterTableWaitRetries(1.second),
    Config.TooManyColumnsRetries(300.seconds)
  )

  def control(mocks: Mocks = Mocks(Nil, Nil)): IO[Control] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      appHealth = testAppHealth(state)
      tableManager <- testTableManager(state, mocks)
    } yield Control(state, tableManager, appHealth)

  private def testAppHealth(state: Ref[IO, Vector[Action]]): AppHealth.Interface[IO, Alert, RuntimeService] =
    new AppHealth.Interface[IO, Alert, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        state.update(_ :+ Action.BecameHealthyForSetup)
      def beUnhealthyForSetup(alert: Alert): IO[Unit] =
        state.update(_ :+ Action.BecameUnhealthyForSetup)
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameHealthy(service))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        state.update(_ :+ Action.BecameUnhealthy(service))
    }

  private def testTableManager(state: Ref[IO, Vector[Action]], mocks: Mocks): IO[TableManager[IO]] =
    for {
      addColsMocksRef <- Ref[IO].of(mocks.addColumnsResults)
      getTableMocksRef <- Ref[IO].of(mocks.getTableResults)
    } yield new TableManager[IO] {
      def addColumns(columns: Vector[Field]): IO[FieldList] =
        for {
          response <- addColsMocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.AddColumnsAttempted(columns))
          result <- response match {
                      case Response.Success =>
                        IO.pure(FieldList.of())
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result

      def tableExists: IO[Boolean] =
        for {
          response <- getTableMocksRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Response.Success)
                      }
          _ <- state.update(_ :+ Action.GetTableAttempted)
          result <- response match {
                      case Response.Success =>
                        IO.pure(true)
                      case Response.ExceptionThrown(ex) =>
                        IO.raiseError(ex).adaptError { t =>
                          t.setStackTrace(Array()) // don't clutter our test logs
                          t
                        }
                    }
        } yield result

      def createTable: IO[Unit] =
        state.update(_ :+ Action.CreateTableAttempted)

    }

  def testGrpcPermissionDeniedSetupError: Throwable = {
    val inner = new StatusRuntimeException(GrpcStatus.PERMISSION_DENIED)
    inner.setStackTrace(Array()) // don't clutter our test logs
    val t = new RuntimeException("go away", inner)
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

  def testAccountNotFoundSetupError: Throwable = {
    val inner = new TestAccountNotFoundException
    inner.setStackTrace(Array()) // don't clutter our test logs
    val t = new RuntimeException("go away", inner)
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

  def testFailedToGetTableError: Throwable =
    new BigQueryException(42, "some message", new BigQueryError("accessdenied", "some location", "some message"))

  private class TestAccountNotFoundException extends HttpResponseException(new HttpResponseException.Builder(400, "", new HttpHeaders())) {
    override def getMessage: String = "-- account not found"
  }

}
