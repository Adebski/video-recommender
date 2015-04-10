package ztis

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._

object TwitterStreamApp extends App {
  Spark.sc
  val ssc = new StreamingContext(Spark.conf, Seconds(1))
  
  ssc.start()
  ssc.awaitTerminationOrTimeout(5.seconds.toMillis)
  ssc.stop()
}
