package ztis

import com.typesafe.config.ConfigFactory

object KafkaTwitterStreamProducer {
  val producer = new KafkaProducer(ConfigFactory.load("twitter-stream"))
}
