package ztis.relationships

import com.typesafe.config.Config
import ztis.KafkaProducer

class KafkaRelationshipFetcherProducer(config: Config) extends RelationshipFetcherProducer {
  private val relationshipFetcherConfig = RelationshipFetcherConfig(config)
  
  private val producer = new KafkaProducer(config)

  def requestRelationshipsForWykopUser(wykopUserID: String): Unit = {
    producer.publish(relationshipFetcherConfig.wykopUsersTopic, wykopUserID)
  }
  
  def requestRelationshipsForTwitterUser(twitterUserID: Long): Unit = {
    val id: java.lang.Long = twitterUserID
    producer.publish(relationshipFetcherConfig.twitterUsersTopic, id)
  }
}
