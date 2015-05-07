package ztis.model

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class SparkSpec extends FlatSpec with BeforeAndAfter {
  private val master = "local[2]"
  private val appName = "spark-test"

  protected var sc: SparkContext = _

  before {

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

}
