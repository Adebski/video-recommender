package ztis.relationships

import akka.actor.ActorRef
import com.typesafe.config.Config
import ztis.KafkaConsumer
import ztis.relationships.RelationshipFetcherActor.{FetchRelationshipsWykop, FetchRelationshipsTwitter}

class KafkaRelationshipFetcherConsumer(config: Config, relationshipFetcherActor: ActorRef) {

  private val relationshipFetcherConfig = RelationshipFetcherConfig(config)

  private val twitterConsumer = new KafkaConsumer(config)

  private val wykopConsumer = new KafkaConsumer(config)
  
  def subscribe(): Unit = {
    subscribeToTwitterUsers()
    subscribeToWykopUsers()
  }

  def shutdown(): Unit = {
    twitterConsumer.shutdown()
    wykopConsumer.shutdown()
  }
  
  private def subscribeToTwitterUsers(): Unit = {
    val threads = relationshipFetcherConfig.twitterUsersThreads
    val topic = relationshipFetcherConfig.twitterUsersTopic
    
    twitterConsumer.subscribe(threads, topic, classOf[java.lang.Long], onTwitterUser)

  }

  private def subscribeToWykopUsers(): Unit = {
    val threads = relationshipFetcherConfig.wykopUsersThreads
    val topic = relationshipFetcherConfig.wykopUsersTopic
    
    wykopConsumer.subscribe(threads, topic, classOf[String], onWykopUser)
  }

  private def onTwitterUser(id: java.lang.Long): Unit = {
    relationshipFetcherActor ! FetchRelationshipsTwitter(id)
  }

  private def onWykopUser(id: String): Unit = {
    relationshipFetcherActor ! FetchRelationshipsWykop(id)
  }
}
