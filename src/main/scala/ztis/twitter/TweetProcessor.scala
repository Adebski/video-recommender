package ztis.twitter

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.Config
import ztis.KafkaConsumer

class TweetProcessor(system: ActorSystem,
                     config: Config,
                     tweetProcessorActor: ActorRef) {
  private val numberOfThreads = config.getInt("kafka-threads")

  private val topic = config.getString("tweet-processor.topic")

  private val consumer = new KafkaConsumer(config)

  consumer.subscribe(numberOfThreads, topic, classOf[Tweet], submit)

  def submit(tweet: Tweet): Unit = {
    tweetProcessorActor ! tweet
  }
}

