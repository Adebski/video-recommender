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

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri") exclude("org.slf4j", "slf4j-simple") exclude("org.slf4j", "slf4j-log4j12")

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.1"

libraryDependencies += "com.twitter" % "twitter-text" % "1.6.1"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "1.1.4"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.4"

net.virtualvoid.sbt.graph.Plugin.graphSettings