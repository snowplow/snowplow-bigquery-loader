/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.effect.{ExitCode, IO, Resource}
import cats.effect.metrics.CpuStarvationWarningMetrics
import io.circe.Decoder
import com.monovore.decline.effect.CommandIOApp
import com.monovore.decline.Opts
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.snowplow.streams.Factory
import com.snowplowanalytics.snowplow.runtime.AppInfo

abstract class LoaderApp[FactoryConfig: Decoder, SourceConfig: Decoder, SinkConfig: Decoder](
  info: AppInfo
) extends CommandIOApp(name = LoaderApp.helpCommand(info), header = info.dockerAlias, version = info.version) {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def onCpuStarvationWarn(metrics: CpuStarvationWarningMetrics): IO[Unit] =
    Logger[IO].debug(s"Cats Effect measured responsiveness in excess of ${metrics.starvationInterval * metrics.starvationThreshold}")

  type FactoryProvider = FactoryConfig => Resource[IO, Factory[IO, SourceConfig, SinkConfig]]

  def toFactory: FactoryProvider

  final def main: Opts[IO[ExitCode]] = Run.fromCli(info, toFactory)

}

object LoaderApp {

  private def helpCommand(appInfo: AppInfo) = s"docker run ${appInfo.dockerAlias}"

}
