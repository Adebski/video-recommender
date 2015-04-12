package ztis

import java.util.concurrent.Executors

import com.typesafe.config.ConfigFactory

object TwitterLinkExtractorApp extends App {
  val config = ConfigFactory.load("twitter-link-extractor")
  val executor = Executors.newFixedThreadPool(config.getInt("worker-threads"))
  
  val consumer = new KafkaConsumer(config, submit)
  val cassandraClient = new CassandraClient(config)
  
  consumer.subscribe("twitter")
  
  def submit(tweet: Tweet): Unit = {
    executor.submit(new ProcessTweetTask(cassandraClient, tweet))
  }
}
