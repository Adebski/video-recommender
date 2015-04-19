package ztis.twitter

import com.typesafe.config.ConfigFactory
import ztis.KafkaProducer

object KafkaTwitterStreamProducer {
  val producer = new KafkaProducer(ConfigFactory.load("twitter-stream"))
}
