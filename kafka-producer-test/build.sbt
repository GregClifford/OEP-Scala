import Dependencies._
import sbt.Keys.libraryDependencies

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val commonScalacOptions = Seq(
  "-encoding", "UTF-8",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:experimental.macros",
  "-language:postfixOps",
  "-Ypartial-unification"
)

lazy val root = (project in file("."))
  .settings(
    name := "kafka-producer-test",
    scalacOptions ++= commonScalacOptions,
    libraryDependencies  ++= List(
      scalaTest % Test,
      "com.spinoco" %% "fs2-kafka" % "0.4.0")
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
