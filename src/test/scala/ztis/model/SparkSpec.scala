package ztis.model

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class SparkSpec extends FlatSpec with BeforeAndAfterAll {
  private val master = "local[2]"
  private val appName = "spark-test"

  protected var sc: SparkContext = _

  override def beforeAll() {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  override def afterAll() = {
    if (sc != null) {
      sc.stop()
    }
  }

}
