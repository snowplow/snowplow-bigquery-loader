/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import com.comcast.ip4s.Port

import scala.concurrent.duration.FiniteDuration
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, HttpClient, Metrics => CommonMetrics, Retrying, Sentry, Telemetry, Webhook}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs.schemaCriterionDecoder
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig

case class Config[+Factory, +Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  streams: Factory,
  batching: Config.Batching,
  cpuParallelism: Config.CpuParallelism,
  retries: Config.Retries,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring,
  license: AcceptedLicense,
  skipSchemas: List[SchemaCriterion],
  legacyColumns: List[SchemaCriterion],
  legacyColumnMode: Boolean,
  exitOnMissingIgluSchema: Boolean,
  decompression: DecompressionConfig,
  http: Config.Http
)

object Config {

  case class WithIglu[+Factory, +Source, +Sink](main: Config[Factory, Source, Sink], iglu: ResolverConfig)

  case class Output[+Sink](good: BigQuery, bad: SinkWithMaxSize[Sink])

  case class SinkWithMaxSize[+Sink](sink: Sink, maxRecordSize: Int)

  case class MaxRecordSize(maxRecordSize: Int)

  case class BigQuery(
    project: String,
    dataset: String,
    table: String,
    credentials: Option[String]
  )

  case class Batching(
    maxBytes: Long,
    maxDelay: FiniteDuration,
    writeBatchConcurrency: Int
  )

  case class CpuParallelism(parseBytesFactor: BigDecimal, transformFactor: BigDecimal)

  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig],
    prometheus: CommonMetrics.PrometheusConfig
  )

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry.Config],
    healthProbe: HealthProbe,
    webhook: Webhook.Config
  )

  case class AlterTableWaitRetries(delay: FiniteDuration)
  case class TooManyColumnsRetries(delay: FiniteDuration)

  case class Retries(
    setupErrors: Retrying.Config.ForSetup,
    transientErrors: Retrying.Config.ForTransient,
    alterTableWait: AlterTableWaitRetries,
    tooManyColumns: TooManyColumnsRetries
  )

  case class Http(client: HttpClient.Config)

  implicit def decoder[Factory: Decoder, Source: Decoder, Sink: Decoder]: Decoder[Config[Factory, Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val sinkWithMaxSize = for {
      sink <- Decoder[Sink]
      maxSize <- deriveConfiguredDecoder[MaxRecordSize]
    } yield SinkWithMaxSize(sink, maxSize.maxRecordSize)
    implicit val bigquery           = deriveConfiguredDecoder[BigQuery]
    implicit val output             = deriveConfiguredDecoder[Output[Sink]]
    implicit val batching           = deriveConfiguredDecoder[Batching]
    implicit val parallelism        = deriveConfiguredDecoder[CpuParallelism]
    implicit val sentryDecoder      = Sentry.ConfigM.sentryDecoder
    implicit val prometheusDecoder  = deriveConfiguredDecoder[CommonMetrics.PrometheusConfig]
    implicit val metricsDecoder     = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]
    implicit val alterTableRetries  = deriveConfiguredDecoder[AlterTableWaitRetries]
    implicit val tooManyColsRetries = deriveConfiguredDecoder[TooManyColumnsRetries]
    implicit val retriesDecoder     = deriveConfiguredDecoder[Retries]
    implicit val httpDecoder        = deriveConfiguredDecoder[Http]

    // TODO add bigquery docs
    implicit val licenseDecoder =
      AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.1/"))

    deriveConfiguredDecoder[Config[Factory, Source, Sink]]
  }

}
