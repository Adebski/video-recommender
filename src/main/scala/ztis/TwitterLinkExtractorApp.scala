package ztis

import com.typesafe.config.ConfigFactory

object TwitterLinkExtractorApp extends App {
  val consumer = new KafkaConsumer(ConfigFactory.load("twitter-link-extractor"))
  
  consumer.subscribe("twitter")
}
