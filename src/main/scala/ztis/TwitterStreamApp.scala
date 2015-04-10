package ztis

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

import scala.concurrent.duration._

object TwitterStreamApp extends App with StrictLogging {
  try {
    val ssc = new StreamingContext(Spark.conf, Seconds(1))
    val tweets = TwitterUtils.createStream(ssc, None, List("t co"))

    tweets.foreachRDD(_.foreach(printTweetAndPushToKafka))

    ssc.start()
    ssc.awaitTerminationOrTimeout(5.seconds.toMillis)
    ssc.stop()

    def printTweetAndPushToKafka(status: Status): Unit = {
      logger.info(status.toString)
      Kafka.publish("twitter", status)
    }
  } catch {
    case e: Exception => logger.error("Error during twitter streaming", e)
  }
}
