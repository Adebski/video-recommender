import sbt.ExclusionRule

name := "video-recommender"

version := "1.0"

scalaVersion := "2.10.5"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xlint", "-Ywarn-dead-code", "-feature")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

val sparkExclusionRules = List(
  ExclusionRule(name = "log4j"),
  ExclusionRule(name = "org.slf4j"),
  ExclusionRule(organization = "org.slf4j"))

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"                % Versions.spark,
  "org.apache.spark" %% "spark-streaming"           % Versions.spark,
  "org.apache.spark" %% "spark-streaming-twitter"   % Versions.spark,
  "org.apache.spark" %% "spark-mllib"               % Versions.spark
).map(_.excludeAll(sparkExclusionRules:_*))

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-rc3"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3"

libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.7.12"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri") exclude("org.slf4j", "slf4j-simple") exclude("org.slf4j", "slf4j-log4j12")

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.1"

libraryDependencies += "com.twitter" % "twitter-text" % "1.6.1"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "1.1.4"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.4"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.5.2"

libraryDependencies += "org.neo4j" % "neo4j" % Versions.neo4j

libraryDependencies +=  "org.neo4j" % "neo4j-kernel" % Versions.neo4j % "test" classifier "tests"

libraryDependencies +=  "org.neo4j" % "neo4j-io" % Versions.neo4j % "test" classifier "tests"

libraryDependencies ++= {
  Seq(
    "io.spray"            %%  "spray-can"     % Versions.spray,
    "io.spray"            %%  "spray-routing" % Versions.spray,
    "io.spray"            %%  "spray-testkit" % Versions.spray  % "test"
  )
}

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka"   %%  "akka-actor"    % Versions.akka,
    "com.typesafe.akka" %% "akka-cluster" % Versions.akka,
    "com.typesafe.akka"   %%  "akka-testkit"  % Versions.akka   % "test"
  )
}

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

Revolver.settings
