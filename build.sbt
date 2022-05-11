import sbt.Keys.libraryDependencies

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.5.0-SNAPSHOT"
ThisBuild / organization     := "com.rulesource"
ThisBuild / organizationName := "rulesource"

lazy val commonScalacOptions = Seq(
  "-encoding", "UTF-8",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:experimental.macros",
  "-language:postfixOps",
  "-Ypartial-unification"
)

val catsRetryVersion = "0.2.7"

lazy val root = (project in file("."))
  .settings(
    name := "oep-kafka-client",
    scalacOptions ++= commonScalacOptions,
    libraryDependencies  ++= List(
      "org.specs2" %% "specs2-core" % "4.3.4"  % Test,
      "com.spinoco" %% "fs2-kafka" % "0.4.0",
      "com.github.cb372" %% "cats-retry-core" % catsRetryVersion,
      "com.github.cb372" %% "cats-retry-cats-effect" % catsRetryVersion)
  )

/*lazy val `kafka-producer-test`  = (project in file("modules/kafka-producer-test"))
  .settings(
    name := "kafka-producer-test",
    scalacOptions ++= commonScalacOptions,
    libraryDependencies  ++= List(
      scalaTest % Test,
      "com.spinoco" %% "fs2-kafka" % "0.4.0")
  )*/