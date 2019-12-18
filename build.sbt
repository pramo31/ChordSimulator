ThisBuild / organization := "cs441.project.chord"
ThisBuild / scalaVersion := "2.13.1"
ThisBuild / version := "0.1.0"
ThisBuild / name := "ChordSimulator"

lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq("log4j" % "log4j" % "1.2.17", "com.typesafe" % "config" % "1.2.1", "commons-lang" % "commons-lang" % "2.6", "org.scalatest" %% "scalatest" % "3.0.8" % "test", "com.typesafe.akka" %% "akka-actor" % "2.5.26", "com.typesafe.akka" %% "akka-http" % "10.1.11", "com.typesafe.akka" %% "akka-stream" % "2.5.26", "io.spray" % "spray-json_2.12" % "1.3.5", "org.json4s" %% "json4s-core" % "3.6.6", "org.json4s" %% "json4s-jackson" % "3.6.6", "org.scalaj" %% "scalaj-http" % "2.4.2", "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0", "com.google.code.gson" % "gson" % "2.8.5","commons-io" % "commons-io" % "2.6")
  )

// Merge strategy to handle deduplicate errors
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in(Compile, run) := Some("cs441.project.chord.SimulationDriver")
mainClass in assembly := Some("cs441.project.chord.SimulationDriver")