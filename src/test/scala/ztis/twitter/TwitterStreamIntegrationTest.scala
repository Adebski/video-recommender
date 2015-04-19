package ztis.twitter

import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import twitter4j.Status
import ztis.Spark

import scala.collection.mutable
import scala.concurrent.duration._

class TwitterStreamIntegrationTest extends FlatSpec {

  val twitterLink = "http://t.co/H27Ftvjoxe"
  val youtubeLink = "https://www.youtube.com/watch?v=PBm8H6NFsGM"

  "Streamed tweets from twitter" should "be pushed to kafka, processed and persisted in Cassandra" in {
    val ssc = Spark.localStreamingContext()
    val queue = mutable.Queue[RDD[Status]]()
    val dstream = ssc.queueStream(queue)

    val user = new TestUser("testUserName", 1)
    val statuses = List(new TestStatus(user, s"text text $twitterLink text", true))
    queue += ssc.sparkContext.makeRDD(statuses)
    TwitterSparkTransformations.pushToKafka(dstream)

    ssc.start()
    ssc.awaitTerminationOrTimeout(10.seconds.toMillis)
    ssc.stop()

  }
}
