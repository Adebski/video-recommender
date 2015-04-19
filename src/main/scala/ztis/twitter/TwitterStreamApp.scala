package ztis.twitter

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
import ztis.Spark

/**
 * Created by adebski on 19.04.15.
 */
object TwitterStreamApp extends App with StrictLogging {
  try {
    val ssc = Spark.localStreamingContext()
    val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, List("t co"))

    tweets.foreachRDD(_.foreach(printTweetAndPushToKafka))

    ssc.start()
    ssc.awaitTerminationOrTimeout(1.minute.toMillis)
    ssc.stop()

    def printTweetAndPushToKafka(status: Status): Unit = {
      logger.debug(status.toString)
      KafkaTwitterStreamProducer.producer.publish("twitter", new Tweet(status))
    }
  } catch {
    case e: Exception => logger.error("Error during twitter streaming", e)
  }
}
