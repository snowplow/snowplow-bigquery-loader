/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

// SBT
import sbt._
import sbt.io.IO
import Keys._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtdynver.DynVerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.snowplowanalytics.snowplow.sbt.IgluSchemaPlugin.autoImport._

import scala.sys.process._

object BuildSettings {

  lazy val commonSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.13.10",
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false,
    scalacOptions += "-Ywarn-macros:after",
    addCompilerPlugin(Dependencies.betterMonadicFor),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with docker

    Compile / resourceGenerators += Def.task {
      val license = (Compile / resourceManaged).value / "META-INF" / "LICENSE"
      IO.copyFile(file("LICENSE.md"), license)
      Seq(license)
    }.taskValue
  )

  lazy val coreSettings = Seq(
    Test / igluUris := Seq(
      // Iglu schemas used in unit tests
      "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
      "iglu:com.snowplowanalytics.snowplow.media/ad_click_event/jsonschema/1-0-0",
      "iglu:com.snowplowanalytics.snowplow.media/ad_break_end_event/jsonschema/1-0-0",
      "iglu:com.snowplowanalytics.iglu/anything-a/jsonschema/1-0-0"
    )
  ) ++ commonSettings

  lazy val appSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](dockerAlias, name, version),
    buildInfoPackage := "com.snowplowanalytics.snowplow.bigquery",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo"),

    // used in configuration parsing unit tests
    Test / envVars := Map(
      "HOSTNAME" -> "test-hostname"
    )
  ) ++ commonSettings ++ addExampleConfToTestCp

  lazy val kafkaSettings = appSettings ++ Seq(
    name := "bigquery-loader-kafka",
    buildInfoKeys += BuildInfoKey("cloud" -> "Azure")
  )

  lazy val pubsubSettings = appSettings ++ Seq(
    name := "bigquery-loader-pubsub",
    buildInfoKeys += BuildInfoKey("cloud" -> "GCP")
  )

  lazy val kinesisSettings = appSettings ++ Seq(
    name := "bigquery-loader-kinesis",
    buildInfoKeys += BuildInfoKey("cloud" -> "AWS")
  )

  lazy val addExampleConfToTestCp = Seq(
    Test / unmanagedClasspath += {
      if (baseDirectory.value.getPath.contains("distroless")) {
        // baseDirectory is like 'root/modules/distroless/module',
        // we're at 'module' and need to get to 'root/config/'
        baseDirectory.value.getParentFile.getParentFile.getParentFile / "config"
      } else {
        // baseDirectory is like 'root/modules/module',
        // we're at 'module' and need to get to 'root/config/'
        baseDirectory.value.getParentFile.getParentFile / "config"
      }
    }
  )
}
