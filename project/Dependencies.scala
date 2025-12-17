/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
import sbt._

object Dependencies {

  object V {
    // Scala
    val catsEffect       = "3.5.4"
    val http4s           = "0.23.17"
    val decline          = "2.4.1"
    val circe            = "0.14.6"
    val circeExtra       = "0.14.4"
    val betterMonadicFor = "0.3.1"
    val doobie           = "1.0.0-RC4"

    // java
    val slf4j           = "2.0.17"
    val azureSdk        = "1.18.1"
    val sentry          = "6.25.2"
    val awsSdk2         = "2.40.9"
    val bigqueryStorage = "3.19.0"
    val bigquery        = "2.57.1"
    val netty           = "4.1.130.Final"
    val jsonSmart       = "2.6.0"

    // Snowplow
    val streams    = "0.18.0"
    val igluClient = "4.0.3"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

  }

  val blazeClient       = "org.http4s"   %% "http4s-blaze-client"  % V.http4s
  val decline           = "com.monovore" %% "decline-effect"       % V.decline
  val circeGenericExtra = "io.circe"     %% "circe-generic-extras" % V.circeExtra
  val circeLiteral      = "io.circe"     %% "circe-literal"        % V.circe
  val betterMonadicFor  = "com.olegpy"   %% "better-monadic-for"   % V.betterMonadicFor
  val doobie            = "org.tpolecat" %% "doobie-core"          % V.doobie

  // java
  val slf4j           = "org.slf4j"              % "slf4j-simple"                 % V.slf4j
  val julToSlf4j      = "org.slf4j"              % "jul-to-slf4j"                 % V.slf4j
  val azureIdentity   = "com.azure"              % "azure-identity"               % V.azureSdk
  val sentry          = "io.sentry"              % "sentry"                       % V.sentry
  val stsSdk2         = "software.amazon.awssdk" % "sts"                          % V.awsSdk2
  val bigqueryStorage = "com.google.cloud"       % "google-cloud-bigquerystorage" % V.bigqueryStorage
  val bigquery        = "com.google.cloud"       % "google-cloud-bigquery"        % V.bigquery
  val nettyHandler    = "io.netty"               % "netty-handler"                % V.netty
  val nettyCommon     = "io.netty"               % "netty-common"                 % V.netty
  val jsonSmart       = "net.minidev"            % "json-smart"                   % V.jsonSmart

  val streamsCore      = "com.snowplowanalytics" %% "streams-core"             % V.streams
  val kinesis          = "com.snowplowanalytics" %% "kinesis"                  % V.streams
  val kafka            = "com.snowplowanalytics" %% "kafka"                    % V.streams
  val pubsub           = "com.snowplowanalytics" %% "pubsub"                   % V.streams
  val loaders          = "com.snowplowanalytics" %% "loaders-common"           % V.streams
  val runtime          = "com.snowplowanalytics" %% "runtime-common"           % V.streams
  val igluClientHttp4s = "com.snowplowanalytics" %% "iglu-scala-client-http4s" % V.igluClient

  // tests
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2           % Test
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % V.catsEffectSpecs2 % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect       % Test

  val coreDependencies = Seq(
    streamsCore,
    loaders,
    runtime,
    igluClientHttp4s,
    bigqueryStorage,
    bigquery,
    blazeClient,
    decline,
    sentry,
    doobie,
    circeGenericExtra,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    circeLiteral % Test,
    julToSlf4j,
    slf4j % Runtime
  )

  val kafkaDependencies = Seq(
    kafka,
    slf4j      % Runtime,
    julToSlf4j % Runtime,
    azureIdentity,
    nettyHandler, // for security vulnerabilities
    jsonSmart, // for security vulnerabilities
    specs2,
    catsEffectSpecs2
  )

  val pubsubDependencies = Seq(
    pubsub,
    slf4j      % Runtime,
    julToSlf4j % Runtime,
    nettyCommon, // for security vulnerabilities
    specs2,
    catsEffectSpecs2
  )

  val kinesisDependencies = Seq(
    kinesis,
    slf4j      % Runtime,
    julToSlf4j % Runtime,
    stsSdk2    % Runtime,
    specs2,
    catsEffectSpecs2
  )

}
