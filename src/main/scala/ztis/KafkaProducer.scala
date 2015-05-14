package ztis

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.producer.{KafkaProducer => KProducer, ProducerRecord}

class KafkaProducer(config: Config) extends StrictLogging with KafkaComponent {

  private val producer = new KProducer[Array[Byte], Array[Byte]](producerConfig(config))

  private val kryo = kryoPool(numberOfInstances = 1)
  
  def publish(topic: String, message: AnyRef): Unit = {
    logger.debug(s"event: $message published to Kafka")
    val bytes = kryo.toBytesWithoutClass(message)
    val msg = new ProducerRecord[Array[Byte], Array[Byte]](topic, bytes)
    producer.send(msg)
  }

  sys.addShutdownHook(producer.close())
}
