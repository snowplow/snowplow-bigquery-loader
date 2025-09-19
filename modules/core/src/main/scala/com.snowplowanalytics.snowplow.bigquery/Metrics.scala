/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.Functor
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.implicits._
import fs2.Stream

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

import scala.concurrent.duration.{Duration, FiniteDuration}

trait Metrics[F[_]] {
  def addGood(count: Long): F[Unit]
  def addBad(count: Long): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(e2eLatency: FiniteDuration): F[Unit]

  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics, sourceAndAck: SourceAndAck[F]): F[Metrics[F]] =
    Ref.ofEffect(State.initialize(sourceAndAck)).map(impl(config, _, sourceAndAck))

  private case class State(
    good: Long,
    bad: Long,
    latency: FiniteDuration,
    e2eLatency: Option[FiniteDuration]
  ) extends CommonMetrics.State {
    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.CountGood(good),
        KVMetric.CountBad(bad),
        KVMetric.Latency(latency)
      ) ++ e2eLatency.map(KVMetric.E2ELatency(_))
  }

  private object State {
    def initialize[F[_]: Functor](sourceAndAck: SourceAndAck[F]): F[State] =
      sourceAndAck.currentStreamLatency.map { latency =>
        State(0L, 0L, latency.getOrElse(Duration.Zero), None)
      }
  }

  private def impl[F[_]: Async](
    config: Config.Metrics,
    ref: Ref[F, State],
    sourceAndAck: SourceAndAck[F]
  ): Metrics[F] =
    new CommonMetrics[F, State](ref, State.initialize(sourceAndAck), config.statsd) with Metrics[F] {
      def addGood(count: Long): F[Unit] =
        ref.update(s => s.copy(good = s.good + count))
      def addBad(count: Long): F[Unit] =
        ref.update(s => s.copy(bad = s.bad + count))
      def setLatency(latency: FiniteDuration): F[Unit] =
        ref.update(s => s.copy(latency = s.latency.max(latency)))
      def setE2ELatency(e2eLatency: FiniteDuration): F[Unit] =
        ref.update { state =>
          val newLatency = state.e2eLatency.fold(e2eLatency)(_.max(e2eLatency))
          state.copy(e2eLatency = Some(newLatency))
        }
    }

  private object KVMetric {

    final case class CountGood(v: Long) extends CommonMetrics.KVMetric {
      val key        = "events_good"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountBad(v: Long) extends CommonMetrics.KVMetric {
      val key        = "events_bad"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class Latency(v: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "latency_millis"
      val value      = v.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class E2ELatency(v: FiniteDuration) extends CommonMetrics.KVMetric {
      val key        = "e2e_latency_millis"
      val value      = v.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }
  }
}
