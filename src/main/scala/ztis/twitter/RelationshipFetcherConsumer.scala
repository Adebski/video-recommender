package ztis.twitter

import akka.actor.ActorRef
import com.typesafe.config.Config
import ztis.KafkaConsumer
import ztis.twitter.RelationshipFetcherActor.FetchRelationships

class RelationshipFetcherConsumer(config: Config, relationshipsFetcherActor: ActorRef) {
  private val topic = config.getString("twitter-relationship-fetcher.topic")

  private val threads = config.getInt("twitter-relationship-fetcher.kafka-threads")

  private val consumer = new KafkaConsumer(config)

  def subscribe(): Unit = {
    consumer.subscribe(threads, topic, classOf[java.lang.Long], submit)
  }

  def submit(id: java.lang.Long): Unit = {
    relationshipsFetcherActor ! FetchRelationships(id)
  }
}
