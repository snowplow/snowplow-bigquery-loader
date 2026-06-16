/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import fs2.Stream

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

import scala.concurrent.duration.FiniteDuration

trait Metrics[F[_]] {
  def addGood(count: Long): F[Unit]
  def addBad(count: Long): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(e2eLatency: FiniteDuration): F[Unit]

  def scrape: F[String]
  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](
    config: Config.Metrics,
    sourceAndAck: SourceAndAck[F]
  ): Resource[F, Metrics[F]] =
    CommonMetrics.build(config.statsd, config.prometheus).evalMap { entries =>
      for {
        goodCounter <- entries.counter("events_good")
        badCounter <- entries.counter("events_bad")
        latencyTimer <- entries.timer("latency_millis", sourceAndAck.currentStreamLatency)
        e2eTimer <- entries.timer("e2e_latency_millis", Sync[F].pure(None))
      } yield new Metrics[F] {
        def addGood(count: Long): F[Unit]                      = goodCounter.add(count)
        def addBad(count: Long): F[Unit]                       = badCounter.add(count)
        def setLatency(latency: FiniteDuration): F[Unit]       = latencyTimer.record(latency)
        def setE2ELatency(e2eLatency: FiniteDuration): F[Unit] = e2eTimer.record(e2eLatency)

        def scrape: F[String]          = entries.scrape
        def report: Stream[F, Nothing] = entries.report
      }
    }
}
