package ztis

import java.util.Properties

import com.twitter.chill.{KryoInstantiator, KryoPool}
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import kafka.consumer.ConsumerConfig

import scala.collection.JavaConverters._


trait KafkaComponent extends StrictLogging {
  private val kryoInstantiator = (new KryoInstantiator).withRegistrar(new ZTISKryoRegistrar)

  protected def kryoPool(numberOfInstances: Int): KryoPool = {
    KryoPool.withByteArrayOutputStream(numberOfInstances, kryoInstantiator)
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

  private def configToMap(config: Config): java.util.Map[String, Object] = {
    val result = new java.util.HashMap[String, Object]()

    config.entrySet.asScala.foreach { entry =>
      result.put(entry.getKey, entry.getValue.unwrapped.toString)
    }

    result
  }

  def consumerConfig(config: Config): ConsumerConfig = {
    val properties = configToProperties(config.getConfig("consumer"))

    new ConsumerConfig(properties)
  }

  def producerConfig(config: Config): java.util.Map[String, Object] = {
    val result = configToMap(config.getConfig("producer"))

    result
  }
}
