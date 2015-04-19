package ztis.wykop

import com.typesafe.config.ConfigFactory
import ztis.KafkaProducer

object KafkaWykopStreamProducer {
  val producer = new KafkaProducer(ConfigFactory.load("wykop"))
}
