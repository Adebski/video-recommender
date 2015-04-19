package ztis.wykop

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.Spark

import scala.concurrent.duration._

object WykopStreamApp extends App with StrictLogging {
  try {
    val config = ConfigFactory.load("wykop")
    val topic = config.getString("wykop.topic")
    val api = new WykopAPI(config)
    val receiver = new WykopSparkReceiver(config, api)
    val ssc = Spark.localStreamingContext()

    val entries = ssc.receiverStream(receiver)

    WykopSparkTransformations.pushToKafka(entries, topic)

    ssc.start()
    ssc.awaitTerminationOrTimeout(1.minute.toMillis)
    ssc.stop()

  } catch {
    case e: Exception => logger.error("Error during wykop streaming", e)
  }
}
