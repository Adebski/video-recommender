package ztis.wykop

import com.typesafe.config.Config
import ztis.KafkaProducer

class RelationshipFetcherProducer(config: Config) {
  private val topic = config.getString("wykop-relationship-fetcher.topic")
  
  private val producer = new KafkaProducer(config)

  /**
   * Queues request for relationships for given wykop user. 
   * @param wykopUserID
   */
  def requestRelationshipsFor(wykopUserID: String): Unit = {
    producer.publish(topic, wykopUserID)
  }
}
