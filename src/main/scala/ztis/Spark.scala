package ztis

import org.apache.spark.{SparkContext, SparkConf}

object Spark {
  lazy val conf = new SparkConf(false) // skip loading external settings
    .setMaster(s"local[2]")
    .setAppName("spark-twitter")
    .set("spark.logConf", "false")
    .set("spark.driver.host", "localhost")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  lazy val sc = new SparkContext(conf)
}
