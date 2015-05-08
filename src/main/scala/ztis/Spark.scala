package ztis

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Spark {
  def baseConfiguration(appName: String) = new SparkConf(false) // skip loading external settings
    .setMaster(s"local[2]")
    .setAppName(appName)
    .set("spark.logConf", "false")
    .set("spark.driver.host", "localhost")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "ztis.ZTISKryoRegistrar")

  def sparkContext(conf: SparkConf): SparkContext = {
    new SparkContext(conf)
  }
  
  def streamingContext(batchDuration: Duration = Seconds(1), conf: SparkConf): StreamingContext = {
    new StreamingContext(conf, batchDuration)
  }
}
