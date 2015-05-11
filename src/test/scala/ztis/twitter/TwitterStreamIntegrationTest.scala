package ztis.twitter

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import kafka.admin.TopicCommand
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import twitter4j.Status
import ztis._
import ztis.cassandra.{CassandraConfiguration, CassandraClient}

import scala.collection.mutable
import scala.concurrent.duration._

class TwitterStreamIntegrationTest extends FlatSpec with BeforeAndAfterAll with StrictLogging {

  val twitterLink = "http://t.co/H27Ftvjoxe"
  val youtubeLink = "https://www.youtube.com/watch?v=PBm8H6NFsGM"
  val cassandraConfig = CassandraConfiguration(ConfigFactory.load("cassandra.conf"))
  val twitterLinkExtractorConf = ConfigFactory.load("twitter-link-extractor.conf")
  val twitterStreamConf = ConfigFactory.load("twitter-stream.conf")
  val testTopic = twitterStreamConf.getString("twitter-stream.topic")
  val cassandraClient = new CassandraClient(cassandraConfig)
  val linkExtractor = new TwitterLinkExtractor(twitterLinkExtractorConf, cassandraClient)
  val repository = new UserAndRatingRepository(cassandraClient)
  val ssc = Spark.streamingContext(conf = Spark.baseConfiguration("twitter-stream-integration-test"))
  
  "Streamed tweets from twitter" should "be pushed to kafka, processed and persisted in Cassandra" in {
    val queue = mutable.Queue[RDD[Status]]()
    val dstream = ssc.queueStream(queue)

    val userName = "testUserName" 
    val user = new TestUser(userName, 1)
    val statuses = List(new TestStatus(user, s"text text $twitterLink text", true))
    queue += ssc.sparkContext.makeRDD(statuses)
    TwitterSparkTransformations.pushToKafka(dstream, testTopic)

    ssc.start()
    ssc.awaitTermination(10.seconds.toMillis)

    val ratings = repository.allRatings()
    val expectedRatings = Vector(UserAndRating(userName, UserOrigin.Twitter, youtubeLink, 1, 0))
    assert(ratings == expectedRatings)
  }

  override protected def afterAll(): Unit = {
    logger.info("Cleaning up ")
    
    logger.info("Stopping spark streaming")
    ssc.stop()
    
    cassandraClient.clean()
    
    logger.info(s"Deleting topic $testTopic")
    TopicCommand.main(Array("--zookeeper", 
      twitterLinkExtractorConf.getString("consumer.zookeeper.connect"),
      "--delete", "--topic", testTopic))
  }
}
