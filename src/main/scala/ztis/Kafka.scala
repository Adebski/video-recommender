package ztis

import java.util.{Properties, UUID}

import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.StrictLogging
import kafka.consumer.{ConsumerConfig, Consumer}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import scala.collection.JavaConverters._

object Kafka extends StrictLogging {

  private val config = ConfigFactory.load("kafka.conf")

  private val producer = new Producer[AnyRef, AnyRef](producerConfig(config))
  //private val consumer = Consumer.create(consumerConfig(config))

  def publish(topic: String, message: AnyRef): Unit = {
    logger.debug(s"event: $message published to Kafka")
    //TODO - implement serialization
    producer.send(new KeyedMessage(topic, "aaa"))
  }

  private def configToProperties(config: Config, extra: Map[String, String] = Map.empty): Properties = {
    val properties = new Properties()

    config.entrySet.asScala.foreach { entry =>
      properties.put(entry.getKey, entry.getValue.unwrapped.toString)
    }

    extra.foreach {
      case (k, v) => properties.put(k, v)
    }

    properties
  }

  def consumerConfig(config: Config): ConsumerConfig = {
    val properties = configToProperties(config.getConfig("cqrs.kafka-pub-sub.consumer"))
    properties.put("group.id", config.getString("cqrs.kafka-pub-sub.node-name"))
    new ConsumerConfig(properties)
  }

  def producerConfig(config: Config): ProducerConfig = {
    val properties = configToProperties(config.getConfig("producer"))
    properties.put("client.id", "twitter")

    new ProducerConfig(properties)
  }
}