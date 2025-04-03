/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.{Applicative, Eq}
import cats.effect.Sync
import cats.implicits._
import com.google.cloud.bigquery.BigQueryException
import com.google.api.client.http.HttpResponseException
import io.grpc.{Status => GrpcStatus, StatusRuntimeException}
import retry._

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying, SetupExceptionMessages}
import com.snowplowanalytics.snowplow.bigquery.{Alert, Config, RuntimeService}
import com.snowplowanalytics.snowplow.bigquery.processing.BigQueryUtils.BQExceptionSyntax

object BigQueryRetrying {

  def withRetries[F[_]: Sync: Sleep, A](
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    config: Config.Retries,
    toAlert: SetupExceptionMessages => Alert
  )(
    action: F[A]
  ): F[A] =
    Retrying.withRetries(appHealth, config.transientErrors, config.setupErrors, RuntimeService.BigQueryClient, toAlert, isSetupError)(
      action
    )

  def isSetupError: PartialFunction[Throwable, String] = {
    case bqe: BigQueryException if Set("notfound", "accessdenied").contains(bqe.lowerCaseReason) =>
      bqe.getMessage
    case sre: StatusRuntimeException if sre.getStatus.getCode === GrpcStatus.Code.PERMISSION_DENIED =>
      sre.getMessage
    case e: HttpResponseException if Option(e.getMessage).exists(_.contains("account not found")) =>
      "Service account doesn't exist or is disabled"
  }

  def policyForAlterTableWait[F[_]: Applicative](config: Config.Retries): RetryPolicy[F] =
    RetryPolicies.fibonacciBackoff[F](config.alterTableWait.delay)

  private implicit def statusCodeEq: Eq[GrpcStatus.Code] = Eq.fromUniversalEquals

}
