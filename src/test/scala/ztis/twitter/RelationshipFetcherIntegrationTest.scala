package ztis.twitter

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKitBase}
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.mock.MockitoSugar
import ztis._
import ztis.cassandra.{CassandraSpec, SparkCassandraClient, UserVideoRatingRepository}
import ztis.user_video_service.UserServiceActor.{RegisterTwitterUser, TwitterUserRegistered}
import ztis.user_video_service.VideoServiceActor.{RegisterVideos, Video, VideosRegistered}
import ztis.user_video_service.persistence._
import ztis.user_video_service.{UserServiceActor, VideoServiceActor}

import scala.concurrent.duration._

class RelationshipFetcherIntegrationTest extends CassandraSpec(ConfigFactory.load("cassandra.conf")) with TestKitBase with ImplicitSender with MockitoSugar {
  lazy val fetcherConfig = ConfigFactory.load("twitter-relationship-fetcher.conf")
  lazy val system = ActorSystem("test-actor-system", fetcherConfig)
  val graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase()
  val metadataRepository = new MetadataRepository(graphDb)
  val schemaInitializer = new SchemaInitializer(graphDb, Option(10.seconds))
  val userRepository = new UserRepository(graphDb)
  val videoRepository = new VideoRepository(graphDb)
  val userVideoRatingRepository = new UserVideoRatingRepository(cassandraClient)
  val testTopic = fetcherConfig.getString("twitter-relationship-fetcher.topic")
  val api = mock[FollowersAPI]
  val sparkConf = Spark.baseConfiguration("SparkCassandraClientTest")
  SparkCassandraClient.setCassandraConfig(sparkConf, cassandraConfig)
  val spark = Spark.sparkContext(conf = sparkConf)
  val sparkCassandraClient = new SparkCassandraClient(cassandraClient, spark)

  "RelationshipFetcher" should "fetch queued relationships and update database with implicit video associations" in {
    // given
    val userServiceActor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository), "user-service-actor")
    val videoServiceActor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository), "video-service-actor")
    val fetcherActor = system.actorOf(RelationshipFetcherActor.props(api, userServiceActor, sparkCassandraClient), "relationship-fetcher")
    val producer = new RelationshipFetcherProducer(fetcherConfig)
    val consumer = new RelationshipFetcherConsumer(fetcherConfig, fetcherActor)
    consumer.subscribe()
    val registerFirstTwitterUser = RegisterTwitterUser("twitter-user", 1)

    userServiceActor ! registerFirstTwitterUser
    val userRegistered = expectMsgClass(classOf[TwitterUserRegistered])
    videoServiceActor ! RegisterVideos(Vector(Video(VideoOrigin.YouTube, URI.create("https://www.youtube.com/watch?v=PBm8H6NFsGM"))))
    val videosRegistered = expectMsgClass(classOf[VideosRegistered])
    val userVideoRating =
      UserVideoRating(userRegistered.internalUserID, UserOrigin.Twitter, videosRegistered.internalVideoIDs(0), VideoOrigin.YouTube, 1)
    val userAssociation = UserVideoImplicitAssociation(userRegistered.internalUserID + 1,
      UserOrigin.Twitter,
      userRegistered.internalUserID,
      UserOrigin.Twitter,
      videosRegistered.internalVideoIDs(0),
      VideoOrigin.YouTube)

    cassandraClient.updateRating(userVideoRating)

    when(api.followersFor(1L)).thenReturn(Vector(TwitterUser(2, "second-twitter-user")))

    // when
    producer.requestRelationshipsFor(registerFirstTwitterUser.externalUserID)
    Thread.sleep(10.seconds.toMillis)

    // then
    assert(userVideoRatingRepository.allRatings().toSet == Set(userVideoRating))
    assert(userVideoRatingRepository.allAssociations().toSet == Set(userAssociation))
  }

  override def beforeAll(): Unit = {
    GlobalGraphOperations.cleanDatabase(graphDb)
    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer, metadataRepository)
    KafkaUtils.createTopic(fetcherConfig.getString("consumer.zookeeper.connect"), testTopic)
  }

  override def afterAll(): Unit = {
    logger.info(s"Deleting topic $testTopic")
    KafkaUtils.removeTopic(fetcherConfig.getString("consumer.zookeeper.connect"), testTopic)
    /*stopping only sparkContext and not Cassandra because CassandraSpec shuts it down
    and it sometimes causes problems 
     */
    sparkCassandraClient.sparkContext.stop()
    super.afterAll()
  }
}
