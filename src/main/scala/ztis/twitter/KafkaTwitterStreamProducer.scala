package ztis.twitter

import com.typesafe.config.ConfigFactory
import ztis.KafkaProducer

/**
 * Created by adebski on 19.04.15.
 */
object KafkaTwitterStreamProducer {
  val producer = new KafkaProducer(ConfigFactory.load("twitter-stream"))
}
