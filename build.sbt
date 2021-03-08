import Dependencies._

ThisBuild / scalaVersion := "2.12.11"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "filterCareers",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.1",
    libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.1",
       libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.46"
  )

  // a bit of magic, ty Tyler
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
