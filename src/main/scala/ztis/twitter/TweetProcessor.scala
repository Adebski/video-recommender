package ztis.twitter

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.Config
import ztis.KafkaConsumer
import ztis.cassandra.CassandraClient

class TweetProcessor(system: ActorSystem,
                     config: Config,
                     cassandraClient: CassandraClient,
                     userServiceActor: ActorRef,
                     videoServiceActor: ActorRef) {
  private val numberOfThreads = config.getInt("kafka-threads")

  private val topic = config.getString("twitter-link-extractor.topic")

  private val consumer = new KafkaConsumer(config)

  consumer.subscribe(numberOfThreads, topic, classOf[Tweet], submit)

  def submit(tweet: Tweet): Unit = {
    system.actorOf(TweetProcessorActor.props(tweet, cassandraClient, userServiceActor = userServiceActor, videoServiceActor = videoServiceActor))
  }
}

