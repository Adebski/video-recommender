package ztis.twitter

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import kafka.admin.TopicCommand
import org.apache.spark.rdd.RDD
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import twitter4j.Status
import ztis._
import ztis.cassandra.{UserAndRatingRepository, CassandraConfiguration, CassandraClient}
import ztis.user_video_service.{UserServiceActor, VideoServiceActor}
import ztis.user_video_service.persistence._

import scala.collection.mutable
import scala.concurrent.duration._

class TwitterStreamIntegrationTest extends FlatSpec with BeforeAndAfterAll with StrictLogging {

  val twitterLink = "http://t.co/H27Ftvjoxe"
  val youtubeLink = "https://www.youtube.com/watch?v=PBm8H6NFsGM"
  val cassandraConfig = CassandraConfiguration(ConfigFactory.load("cassandra.conf"))
  val tweetProcessorConfig = ConfigFactory.load("tweet-processor.conf")
  val twitterStreamConf = ConfigFactory.load("twitter-stream.conf")
  val testTopic = twitterStreamConf.getString("twitter-stream.topic")

  val system = ActorSystem("TestSystem", tweetProcessorConfig)
  val graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase()
  val metadataRepository = new MetadataRepository(graphDb)
  val schemaInitializer = new SchemaInitializer(graphDb, Option(10.seconds))
  val userRepository = new UserRepository(graphDb)
  val videoRepository = new VideoRepository(graphDb)
  val cassandraClient = new CassandraClient(cassandraConfig)
  
  val repository = new UserAndRatingRepository(cassandraClient)
  val ssc = Spark.streamingContext(conf = Spark.baseConfiguration("twitter-stream-integration-test"))
  
  "Streamed tweets from twitter" should "be pushed to kafka, processed and persisted in Cassandra" in {
    val userService = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))
    val videoService = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
    new TweetProcessor(system, tweetProcessorConfig, cassandraClient, userServiceActor = userService, videoServiceActor = videoService)

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
    val expectedRatings = Vector(UserAndRating(0, UserOrigin.Twitter, 0, VideoOrigin.YouTube, 1, 0))
    assert(ratings == expectedRatings)
  }

  override def beforeAll(): Unit = {
    GlobalGraphOperations.cleanDatabase(graphDb)
    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer, metadataRepository)
  }
  
  override def afterAll(): Unit = {
    logger.info("Cleaning up ")
    
    logger.info("Stopping spark streaming")
    ssc.stop()
    
    cassandraClient.clean()
    system.shutdown()
    
    logger.info(s"Deleting topic $testTopic")
    TopicCommand.main(Array("--zookeeper", 
      tweetProcessorConfig.getString("consumer.zookeeper.connect"),
      "--delete", "--topic", testTopic))
  }
}
