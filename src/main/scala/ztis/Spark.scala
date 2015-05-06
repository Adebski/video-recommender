package ztis

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Spark {
  val localConfiguration = new SparkConf(false) // skip loading external settings
    .setMaster(s"local[2]")
    .setAppName("spark-twitter")
    .set("spark.logConf", "false")
    .set("spark.driver.host", "localhost")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  def localStreamingContext(batchDuration: Duration = Seconds(1)): StreamingContext = {
    new StreamingContext(Spark.localConfiguration, batchDuration)
  }
}
