package ztis.twitter

import com.typesafe.config.Config
import ztis.KafkaProducer

class RelationshipFetcherProducer(config: Config) {
  private val topic = config.getString("twitter-relationship-fetcher.topic")
  
  private val producer = new KafkaProducer(config)
  
  def queueFetchRelationshipsFor(twitterUserID: Long): Unit = {
    val id: java.lang.Long = twitterUserID
    producer.publish(topic, id)
  }
}
