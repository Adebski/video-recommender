package ztis.twitter

import java.util.concurrent.Executors

import com.typesafe.config.Config
import ztis.{CassandraClient, KafkaConsumer}

class TwitterLinkExtractor(config: Config, cassandraClient: CassandraClient) {
  private val executor = Executors.newFixedThreadPool(config.getInt("worker-threads"))

  private val consumer = new KafkaConsumer(config, submit)

  private val topic = config.getString("twitter-link-extractor.topic")
  
  consumer.subscribe(executor, topic, classOf[Tweet], submit)

  def submit(tweet: Tweet): Unit = {
    executor.submit(new ProcessTweetTask(cassandraClient, tweet))
  }
  
}
