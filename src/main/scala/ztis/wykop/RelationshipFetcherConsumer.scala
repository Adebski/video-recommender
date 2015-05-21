package ztis.wykop

import akka.actor.ActorRef
import com.typesafe.config.Config
import ztis.KafkaConsumer
import ztis.wykop.RelationshipFetcherActor.FetchRelationshipsFor

class RelationshipFetcherConsumer(config: Config, wykopRelationshipsFetcherActor: ActorRef) {
  private val topic = config.getString("wykop-relationship-fetcher.topic")

  private val threads = config.getInt("wykop-relationship-fetcher.kafka-threads")

  private val consumer = new KafkaConsumer(config)

  def subscribe(): Unit = {
    consumer.subscribe(threads, topic, classOf[String], submit)
  }

  def submit(userName: String): Unit = {
    wykopRelationshipsFetcherActor ! FetchRelationshipsFor(userName)
  }
}
