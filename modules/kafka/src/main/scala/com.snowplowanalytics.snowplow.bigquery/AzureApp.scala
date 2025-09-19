/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import cats.effect.IO

import com.snowplowanalytics.snowplow.streams.kafka.{KafkaFactory, KafkaSinkConfig, KafkaSourceConfig}

object AzureApp extends LoaderApp[Unit, KafkaSourceConfig, KafkaSinkConfig](BuildInfo) {

  override def toFactory: FactoryProvider = _ => KafkaFactory.resource[IO]

}
