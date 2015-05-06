package ztis.wykop

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object WykopSparkTransformations extends StrictLogging {
  def pushToKafka(entries: RDD[Entry], topic: String): Unit = {
    entries.foreach(tweet => pushToKafka(tweet, topic))
  }

  private def pushToKafka(entry: Entry, topic: String): Unit = {
    logger.debug(entry.toString)
    KafkaWykopStreamProducer.producer.publish(topic, entry)
  }

  def pushToKafka(entries: DStream[Entry], topic: String): Unit = {
    entries.foreachRDD(rdd => pushToKafka(rdd, topic: String))
  }
}
