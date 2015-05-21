package ztis.twitter

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKitBase}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.rdd.RDD
import org.mockito.Mockito._
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.mock.MockitoSugar
import twitter4j.Status
import ztis._
import ztis.cassandra.{CassandraSpec, UserVideoRatingRepository}
import ztis.relationships.RelationshipFetcherProducer
import ztis.user_video_service.UserServiceActor.{CreateRelationshipsToTwitterUser, RegisterTwitterUser, TwitterUserRegistered}
import ztis.user_video_service.persistence._
import ztis.user_video_service.{UserServiceActor, VideoServiceActor}

import scala.collection.mutable
import scala.concurrent.duration._

class TwitterStreamIntegrationTest extends CassandraSpec(ConfigFactory.load("cassandra.conf")) with TestKitBase with MockitoSugar with StrictLogging with ImplicitSender {

  val twitterLink = "http://t.co/H27Ftvjoxe"
  val youtubeLink = "https://www.youtube.com/watch?v=PBm8H6NFsGM"
  lazy val tweetProcessorConfig = ConfigFactory.load("tweet-processor.conf")
  val twitterStreamConf = ConfigFactory.load("twitter-stream.conf")
  val testTopic = twitterStreamConf.getString("twitter-stream.topic")

  lazy val system = ActorSystem("TestSystem", tweetProcessorConfig)
  val graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase()
  val metadataRepository = new MetadataRepository(graphDb)
  val schemaInitializer = new SchemaInitializer(graphDb, Option(10.seconds))
  val userRepository = new UserRepository(graphDb)
  val videoRepository = new VideoRepository(graphDb)
  val relationshipsFetcherProducer = mock[RelationshipFetcherProducer]

  val repository = new UserVideoRatingRepository(cassandraClient)
  val ssc = Spark.streamingContext(conf = Spark.baseConfiguration("twitter-stream-integration-test"))

  "Streamed tweets from twitter" should "be pushed to kafka, processed and persisted in Cassandra" in {
    // given
    val userService = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))
    val videoService = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
    val tweetProcessorActor =
      system.actorOf(TweetProcessorActorSupervisor.props(cassandraClient, userServiceActor = userService, videoServiceActor = videoService, relationshipsFetcherProducer), "tweet-processor-actor-supervisor")
    new TweetProcessor(system, tweetProcessorConfig, tweetProcessorActor)

    // registering some users to check if newly added video is properly propagated to users that are following new user
    val registerFirstUser = RegisterTwitterUser("user-with-no-movies", 1)
    userService ! registerFirstUser
    userService ! RegisterTwitterUser("user-with-no-movies2", 2)
    expectMsgClass(classOf[TwitterUserRegistered])
    expectMsgClass(classOf[TwitterUserRegistered])
    userService ! CreateRelationshipsToTwitterUser(1, fromUsers = Vector(TwitterUser(2, "user-with-no-movies2")))

    val queue = mutable.Queue[RDD[Status]]()
    val dstream = ssc.queueStream(queue)

    val user = new TestUser(registerFirstUser.externalUserName, registerFirstUser.externalUserID)
    val statuses = List(new TestStatus(user, s"text text $twitterLink text", true))
    queue += ssc.sparkContext.makeRDD(statuses)
    TwitterSparkTransformations.pushToKafka(dstream, testTopic)

    ssc.start()
    ssc.awaitTermination(10.seconds.toMillis)

    val ratings = repository.allRatings()
    val associations = repository.allAssociations()
    val expectedRatings = Vector(UserVideoRating(0, UserOrigin.Twitter, 0, VideoOrigin.YouTube, 1))
    val expectedAssociations = Vector(UserVideoImplicitAssociation(1, UserOrigin.Twitter, 0, UserOrigin.Twitter, 0, VideoOrigin.YouTube))
    assert(ratings == expectedRatings)
    assert(associations == expectedAssociations)
    verify(relationshipsFetcherProducer).requestRelationshipsForTwitterUser(registerFirstUser.externalUserID)
  }

  override def beforeAll(): Unit = {
    GlobalGraphOperations.cleanDatabase(graphDb)
    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer, metadataRepository)
    KafkaUtils.createTopic(tweetProcessorConfig.getString("consumer.zookeeper.connect"), testTopic)
  }

  override def afterAll(): Unit = {
    logger.info("Stopping spark streaming")
    ssc.stop()

    logger.info(s"Deleting topic $testTopic")
    KafkaUtils.removeTopic(tweetProcessorConfig.getString("consumer.zookeeper.connect"), testTopic)
    super.afterAll()
  }
}
