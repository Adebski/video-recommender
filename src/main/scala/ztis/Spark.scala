package ztis

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Spark {
  /*
  Passing uiPort parameter because we will have multiple spark instances on single JVM so we 
  want to be able to access the UIs of different instances
  
  4040 is the default value for 1.2.1 Spark
   */
  def baseConfiguration(appName: String, uiPort: Int = 4040) = new SparkConf(false) // skip loading external settings
    .setMaster(s"local[2]")
    .setAppName(appName)
    .set("spark.logConf", "false")
    .set("spark.driver.host", "localhost")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "ztis.ZTISKryoRegistrar")
    .set("spark.ui.port", uiPort.toString)

  def sparkContext(conf: SparkConf): SparkContext = {
    new SparkContext(conf)
  }
  
  def streamingContext(batchDuration: Duration = Seconds(1), conf: SparkConf): StreamingContext = {
    new StreamingContext(conf, batchDuration)
  }
}
