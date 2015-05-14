package ztis

import java.util.concurrent.Executors

import com.twitter.chill.KryoPool
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import kafka.consumer.{Consumer, KafkaStream, Whitelist}
import kafka.serializer.DefaultDecoder

class KafkaConsumer(config: Config) extends KafkaComponent {

  private val consumer = Consumer.create(consumerConfig(config))

  def subscribe[T](numberOfThreads: Int, topic: String, clazz: Class[T], onMessage: T => Unit) = {
    logger.info(s"Subscribing on $numberOfThreads to $topic")
    val filterSpec = new Whitelist(topic)
    val executor = Executors.newFixedThreadPool(numberOfThreads)
    val streams: Seq[KafkaStream[Array[Byte], Array[Byte]]] = consumer.createMessageStreamsByFilter(filterSpec, numberOfThreads, new DefaultDecoder(), new DefaultDecoder())
    val kryo: KryoPool = kryoPool(numberOfThreads)

    streams.foreach { stream =>
      executor.submit(new KafkaConsumerTask(stream, kryo, clazz, onMessage))
    }

    sys.addShutdownHook {
      executor.shutdownNow()
    }
  }

  sys.addShutdownHook {
    consumer.shutdown()
  }
}

class KafkaConsumerTask[T](stream: KafkaStream[Array[Byte], Array[Byte]],
                           kryo: KryoPool,
                           clazz: Class[T],
                           onMessage: T => Unit) extends Runnable with StrictLogging {
  override def run(): Unit = {
    for (messageAndTopic <- stream) {
      val message = kryo.fromBytes(messageAndTopic.message(), clazz)

      logger.debug(s"$message read from Kafka")
      onMessage.apply(message)
    }
  }
}
