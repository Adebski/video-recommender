import sbt.ExclusionRule

name := "video-recommender"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" excludeAll(
  ExclusionRule(name = "log4j"),
  ExclusionRule(name = "org.slf4j"),
  ExclusionRule(organization = "org.slf4j")
  )

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.3.0" excludeAll(
  ExclusionRule(name = "log4j"),
  ExclusionRule(name = "org.slf4j"),
  ExclusionRule(organization = "org.slf4j")
  )

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.3.0" excludeAll(
  ExclusionRule(name = "log4j"),
  ExclusionRule(name = "org.slf4j"),
  ExclusionRule(organization = "org.slf4j")
  )

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3"

libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.7.12"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

net.virtualvoid.sbt.graph.Plugin.graphSettings