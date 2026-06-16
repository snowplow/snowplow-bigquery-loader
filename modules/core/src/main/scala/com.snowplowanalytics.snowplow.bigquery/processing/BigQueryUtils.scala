/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.effect.Sync
import com.google.cloud.bigquery.{BigQueryException, TableId}
import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.Credentials

import com.snowplowanalytics.snowplow.bigquery.Config

import java.nio.charset.StandardCharsets
import java.io.ByteArrayInputStream

object BigQueryUtils {

  val gcpUserAgent: String = "Google/ISV Solution (GPN:isol_plb32_0014m00002tg62aqad_qufvepzajwfju6jxl5uwg2zn3l3nizte)"

  def credentials[F[_]: Sync](config: Config.BigQuery): F[Credentials] =
    config.credentials match {
      case None => Sync[F].blocking(GoogleCredentials.getApplicationDefault)
      case Some(json) =>
        Sync[F].blocking {
          GoogleCredentials.fromStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)))
        }
    }

  def tableIdOf(config: Config.BigQuery): TableId =
    TableId.of(config.project, config.dataset, config.table)

  def streamIdOf(config: Config.BigQuery): String =
    tableIdOf(config).getIAMResourceName + "/streams/_default"

  implicit class BQExceptionSyntax(val bqe: BigQueryException) extends AnyVal {
    def lowerCaseReason: String =
      Option(bqe.getError())
        .flatMap(e => Option(e.getReason))
        .map(_.toLowerCase)
        .getOrElse("")

    def lowerCaseMessage: String =
      Option(bqe.getError())
        .flatMap(e => Option(e.getMessage))
        .map(_.toLowerCase)
        .getOrElse("")

  }
}
