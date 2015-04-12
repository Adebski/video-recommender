package ztis

import com.typesafe.config.Config
import kafka.consumer.{Consumer, Whitelist}
import kafka.serializer.DefaultDecoder

class KafkaConsumer(config: Config, onMessage: Tweet => Unit) extends KafkaComponent {

  private val consumer = Consumer.create(consumerConfig(config))

  def subscribe(topic: String) = {
    val filterSpec = new Whitelist(topic)
    val stream = consumer.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).head
    logger.info(s"Subscribed to $topic topic in Kafka")
    for (messageAndTopic <- stream) {
      val tweet = kryo.fromBytes(messageAndTopic.message(), classOf[Tweet])

      logger.debug(s"tweet: $tweet read from Kafka")
      onMessage.apply(tweet)
    }
  }
  
  sys.addShutdownHook(consumer.shutdown())
}
                      