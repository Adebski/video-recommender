package ztis.twitter

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object TwitterSparkTransformations extends StrictLogging {
  def pushToKafka(tweets: RDD[Status], topic: String): Unit = {
    tweets.foreach(tweet => pushToKafka(tweet, topic))
  }

  private def pushToKafka(tweet: Status, topic: String): Unit = {
    logger.debug(tweet.toString)
    KafkaTwitterStreamProducer.producer.publish(topic, new Tweet(tweet))
  }

  def pushToKafka(tweets: DStream[Status], topic: String): Unit = {
    tweets.foreachRDD(rdd => pushToKafka(rdd, topic: String))
  }
}
