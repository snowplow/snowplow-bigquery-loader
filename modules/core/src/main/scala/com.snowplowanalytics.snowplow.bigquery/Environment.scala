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
import cats.effect.{Async, Resource}
import org.http4s.client.Client
import retry.RetryPolicy

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig
import com.snowplowanalytics.snowplow.bigquery.processing.{BigQueryRetrying, BigQueryUtils, TableManager, Writer}
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe, HttpClient, Sentry, Webhook}

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  resolver: Resolver[F],
  httpClient: Client[F],
  tableManager: TableManager.WithHandledErrors[F],
  writer: Writer.Provider[F],
  metrics: Metrics[F],
  appHealth: AppHealth.Interface[F, Alert, RuntimeService],
  alterTableWaitPolicy: RetryPolicy[F],
  batching: Config.Batching,
  cpuParallelism: CpuParallelism,
  badRowMaxSize: Int,
  schemasToSkip: List[SchemaCriterion],
  legacyColumns: List[SchemaCriterion],
  legacyColumnMode: Boolean,
  exitOnMissingIgluSchema: Boolean,
  decompression: DecompressionConfig
)

case class CpuParallelism(parseBytes: Int, transform: Int)

object Environment {

  def fromConfig[F[_]: Async, FactoryConfig, SourceConfig, SinkConfig](
    config: Config.WithIglu[FactoryConfig, SourceConfig, SinkConfig],
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, SinkConfig]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- Sentry.enable[F](appInfo, config.main.monitoring.sentry)
      factory <- toFactory(config.main.streams)
      sourceAndAck <- factory.source(config.main.input)
      sourceReporter = sourceAndAck.isHealthy(config.main.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, Alert, RuntimeService](List(sourceReporter)))
      metrics <- Metrics.build(config.main.monitoring.metrics, sourceAndAck)
      resolver <- mkResolver[F](config.iglu)
      httpClient <- HttpClient.resource[F](config.main.http.client)
      _ <- HealthProbe.resource(config.main.monitoring.healthProbe.port, appHealth, metrics.scrape)
      _ <- Webhook.resource(config.main.monitoring.webhook, appInfo, httpClient, appHealth)
      badSink <- factory
                   .sink(config.main.output.bad.sink)
                   .onError(_ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)))
      creds <- Resource.eval(BigQueryUtils.credentials(config.main.output.good))
      tableManager <- Resource.eval(TableManager.make(config.main.output.good, creds))
      tableManagerWrapped <- Resource.eval(TableManager.withHandledErrors(tableManager, config.main.retries, appHealth))
      writerBuilder <- Writer.builder(config.main.output.good, creds)
      writerProvider <- Writer.provider(writerBuilder, config.main.retries, appHealth)
      cpuParallelism = CpuParallelism(
                         parseBytes = chooseCpuParallelism(config.main.cpuParallelism.parseBytesFactor),
                         transform  = chooseCpuParallelism(config.main.cpuParallelism.transformFactor)
                       )
    } yield Environment(
      appInfo                 = appInfo,
      source                  = sourceAndAck,
      badSink                 = badSink,
      resolver                = resolver,
      httpClient              = httpClient,
      tableManager            = tableManagerWrapped,
      writer                  = writerProvider,
      metrics                 = metrics,
      appHealth               = appHealth,
      alterTableWaitPolicy    = BigQueryRetrying.policyForAlterTableWait[F](config.main.retries),
      batching                = config.main.batching,
      cpuParallelism          = cpuParallelism,
      badRowMaxSize           = config.main.output.bad.maxRecordSize,
      schemasToSkip           = config.main.skipSchemas,
      legacyColumns           = config.main.legacyColumns,
      legacyColumnMode        = config.main.legacyColumnMode,
      exitOnMissingIgluSchema = config.main.exitOnMissingIgluSchema,
      decompression           = config.main.decompression
    )

  private def mkResolver[F[_]: Async](resolverConfig: Resolver.ResolverConfig): Resource[F, Resolver[F]] =
    Resource.eval {
      Resolver
        .fromConfig[F](resolverConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu resolver config", e))
        .value
        .rethrow
    }

  /**
   * For bigger instances (more cores) we want more parallelism, so that cpu-intensive steps can
   * take advantage of all the cores.
   */
  private def chooseCpuParallelism(factor: BigDecimal): Int =
    (Runtime.getRuntime.availableProcessors * factor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt
}
