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
import ztis.cassandra.{CassandraSpec, UserVideoRatingRepository, CassandraConfiguration, CassandraClient}
import ztis.user_video_service.{UserServiceActor, VideoServiceActor}
import ztis.user_video_service.persistence._

import scala.collection.mutable
import scala.concurrent.duration._

class TwitterStreamIntegrationTest extends CassandraSpec(ConfigFactory.load("cassandra.conf")) with StrictLogging {

  val twitterLink = "http://t.co/H27Ftvjoxe"
  val youtubeLink = "https://www.youtube.com/watch?v=PBm8H6NFsGM"
  val tweetProcessorConfig = ConfigFactory.load("tweet-processor.conf")
  val twitterStreamConf = ConfigFactory.load("twitter-stream.conf")
  val testTopic = twitterStreamConf.getString("twitter-stream.topic")

  val system = ActorSystem("TestSystem", tweetProcessorConfig)
  val graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase()
  val metadataRepository = new MetadataRepository(graphDb)
  val schemaInitializer = new SchemaInitializer(graphDb, Option(10.seconds))
  val userRepository = new UserRepository(graphDb)
  val videoRepository = new VideoRepository(graphDb)
  
  val repository = new UserVideoRatingRepository(cassandraClient)
  val ssc = Spark.streamingContext(conf = Spark.baseConfiguration("twitter-stream-integration-test"))
  
  "Streamed tweets from twitter" should "be pushed to kafka, processed and persisted in Cassandra" in {
    val userService = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))
    val videoService = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
    val tweetProcessorActor = 
      system.actorOf(TweetProcessorActorSupervisor.props(cassandraClient, userServiceActor = userService, videoServiceActor = videoService), "tweet-processor-actor-supervisor")

    new TweetProcessor(system, tweetProcessorConfig, tweetProcessorActor)

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
    val expectedRatings = Vector(UserVideoRating(0, UserOrigin.Twitter, 0, VideoOrigin.YouTube, 1))
    assert(ratings == expectedRatings)
  }

  override def beforeAll(): Unit = {
    GlobalGraphOperations.cleanDatabase(graphDb)
    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer, metadataRepository)
    TopicCommand.main(Array("--zookeeper",
      tweetProcessorConfig.getString("consumer.zookeeper.connect"),
      "--create", "--topic", testTopic, "--partitions", "1", "--replication-factor", "1"))
  }
  
  override def afterAll(): Unit = {
    logger.info("Stopping spark streaming")
    ssc.stop()

    logger.info(s"Deleting topic $testTopic")
    TopicCommand.main(Array("--zookeeper",
      tweetProcessorConfig.getString("consumer.zookeeper.connect"),
      "--delete", "--topic", testTopic))
    super.afterAll()
  }
}
