package ztis

import java.util.concurrent.ExecutorService

import com.twitter.chill.KryoPool
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import kafka.consumer.{Consumer, KafkaStream, Whitelist}
import kafka.serializer.DefaultDecoder
import ztis.twitter.Tweet

class KafkaConsumer(config: Config, onMessage: Tweet => Unit) extends KafkaComponent {

  private val consumer = Consumer.create(consumerConfig(config))

  def subscribe[T](executor: ExecutorService, topic: String, clazz: Class[T], onMessage: T => Unit) = {
    val filterSpec = new Whitelist(topic)
    val stream: KafkaStream[Array[Byte], Array[Byte]] = consumer.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).head
    logger.info(s"Subscribed to $topic topic in Kafka")
    executor.submit(new KafkaConsumerTask(stream, kryo, clazz, onMessage))
  }

  sys.addShutdownHook(consumer.shutdown())
}

class KafkaConsumerTask[T](stream: KafkaStream[Array[Byte], Array[Byte]],
                           kryo: KryoPool,
                           clazz: Class[T],
                           onMessage: T => Unit) extends Runnable with StrictLogging {
  override def run(): Unit = {
    for (messageAndTopic <- stream) {
      val tweet = kryo.fromBytes(messageAndTopic.message(), clazz)

      logger.debug(s"tweet: $tweet read from Kafka")
      onMessage.apply(tweet)
    }
  }
}