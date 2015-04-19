package ztis.twitter

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.streaming.twitter.TwitterUtils
import ztis.Spark

import scala.concurrent.duration._

object TwitterStreamApp extends App with StrictLogging {
  try {
    val ssc = Spark.localStreamingContext()
    val tweets = TwitterUtils.createStream(ssc, None, List("t co"))

    TwitterSparkTransformations.pushToKafka(tweets)

    ssc.start()
    ssc.awaitTerminationOrTimeout(1.minute.toMillis)
    ssc.stop()

  } catch {
    case e: Exception => logger.error("Error during twitter streaming", e)
  }
}
