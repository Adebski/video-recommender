package ztis.twitter

import java.net.URI

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status
import ztis.VideoOrigin

object TwitterSparkTransformations extends StrictLogging {
  def pushToKafka(tweets: RDD[Status], topic: String): Unit = {
    tweets.foreach(tweet => pushToKafka(tweet, topic))
  }

  private def pushToKafka(tweet: Status, topic: String): Unit = {
    logger.debug(tweet.toString)
    val links = tweet.getURLEntities
    val videoURIs =
      links.flatMap(entity => VideoOrigin.normalizeVideoUrl(entity.getExpandedURL))
           .map(videoUrl => URI.create(videoUrl))

    if (videoURIs.nonEmpty) {
      val videoLinksStrings = videoURIs.map(_.toString)
      val toPublish = new Tweet(tweet.getUser.getId, tweet.getUser.getName, videoLinksStrings, tweet.isRetweet)
      logger.info(s"Publishing $toPublish")
      KafkaTwitterStreamProducer.producer.publish(topic, toPublish)
    }
  }

  def pushToKafka(tweets: DStream[Status], topic: String): Unit = {
    tweets.foreachRDD(rdd => pushToKafka(rdd, topic: String))
  }
}
