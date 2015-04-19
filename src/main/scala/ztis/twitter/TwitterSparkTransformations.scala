package ztis.twitter

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object TwitterSparkTransformations extends StrictLogging {
  def pushToKafka(tweets: RDD[Status]): Unit = {
    tweets.foreach(pushToKafka)
  }

  private def pushToKafka(tweet: Status): Unit = {
    logger.debug(tweet.toString)
    KafkaTwitterStreamProducer.producer.publish("twitter", new Tweet(tweet))
  }

  def pushToKafka(tweets: DStream[Status]): Unit = {
    tweets.foreachRDD(rdd => pushToKafka(rdd))
  }
}
