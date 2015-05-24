package ztis.twitter

import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.twitter.TwitterUtils
import ztis.{Initializer, Spark}

class TwitterStreamInitializer extends Initializer {
  override def initialize(): Unit = {
    try {
      val config = ConfigFactory.load("twitter-stream")
      val topic = config.getString("twitter-stream.topic")
      val ssc = Spark.streamingContext(conf = Spark.baseConfiguration("twitter-stream", uiPort = 4043))
      val tweets = TwitterUtils.createStream(ssc, None, List("t co"))

      TwitterSparkTransformations.pushToKafka(tweets, topic)

      ssc.start()
      //ssc.awaitTerminationOrTimeout(1.minute.toMillis)
      ssc.awaitTermination()
      ssc.stop()

    } catch {
      case e: Exception => logger.error("Error during twitter streaming", e)
    }
  }
}
