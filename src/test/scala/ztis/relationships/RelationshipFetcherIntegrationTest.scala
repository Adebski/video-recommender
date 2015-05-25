package ztis.relationships

import java.net.URI

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKitBase}
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.mock.MockitoSugar
import ztis._
import ztis.cassandra.{CassandraSpec, SparkCassandraClient, UserVideoRatingRepository}
import ztis.twitter.TwitterUser
import ztis.user_video_service.UserServiceActor.{WykopUserRegistered, RegisterWykopUser, RegisterTwitterUser, TwitterUserRegistered}
import ztis.user_video_service.VideoServiceActor.{RegisterVideos, Video, VideosRegistered}
import ztis.user_video_service.persistence._
import ztis.user_video_service.{UserServiceActor, VideoServiceActor}
import ztis.wykop.WykopAPI

import scala.concurrent.duration._

class RelationshipFetcherIntegrationTest extends CassandraSpec(ConfigFactory.load("cassandra.conf")) with TestKitBase with ImplicitSender with MockitoSugar {
  lazy val fetcherConfig = ConfigFactory.load("relationship-fetcher.conf")
  lazy val system = ActorSystem("test-actor-system", fetcherConfig)
  val graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase()
  val metadataRepository = new MetadataRepository(graphDb)
  val schemaInitializer = new SchemaInitializer(graphDb, Option(10.seconds))
  val userRepository = new UserRepository(graphDb)
  val videoRepository = new VideoRepository(graphDb)
  val userVideoRatingRepository = new UserVideoRatingRepository(cassandraClient)
  val twitterTestTopic = fetcherConfig.getString("relationship-fetcher.twitter-users-topic")
  val wykopTestTopic = fetcherConfig.getString("relationship-fetcher.wykop-users-topic")
  val twitterAPI = mock[TwitterFollowersAPI]
  val wykopAPI = mock[WykopAPI]
  val sparkConf = Spark.baseConfiguration("SparkCassandraClientTest")
  SparkCassandraClient.setCassandraConfig(sparkConf, cassandraConfig)
  val spark = Spark.sparkContext(conf = sparkConf)
  val sparkCassandraClient = new SparkCassandraClient(cassandraClient, spark)

  "RelationshipFetcher" should "fetch queued relationships and update database with implicit video associations" in {
    // given
    val userServiceActor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository), "user-service-actor")
    val videoServiceActor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository), "video-service-actor")
    val twitterFetcherActor = system.actorOf(TwitterRelationshipFetcherActor.props(twitterAPI, userServiceActor, sparkCassandraClient), "twitter-relationship-fetcher")
    val wykopFetcherActor = system.actorOf(WykopRelationshipFetcherActor.props(wykopAPI, userServiceActor, sparkCassandraClient), "wykop-relationship-fetcher")
    val producer = new KafkaRelationshipFetcherProducer(fetcherConfig)
    val consumer = new KafkaRelationshipFetcherConsumer(fetcherConfig, 
      twitterRelationshipFetcher = twitterFetcherActor,
      wykopRelationshipFetcher = wykopFetcherActor)
    consumer.subscribe()
    
    val registerFirstTwitterUser = RegisterTwitterUser("twitter-user", 1)
    val registerFirstWykopUser = RegisterWykopUser("wykop-user")
    
    userServiceActor ! registerFirstTwitterUser
    userServiceActor ! registerFirstWykopUser
    val twitterUserRegistered = expectMsgClass(classOf[TwitterUserRegistered])
    val wykopUserRegistered = expectMsgClass(classOf[WykopUserRegistered])
    videoServiceActor ! RegisterVideos(Vector(
      Video(VideoOrigin.YouTube, URI.create("https://www.youtube.com/watch?v=PBm8H6NFsGM")),
      Video(VideoOrigin.Vimeo, URI.create("https://www.vimeo.com"))
    ))
    val videosRegistered = expectMsgClass(classOf[VideosRegistered])
    val twitterUserVideoRating =
      UserVideoRating(twitterUserRegistered.internalUserID, UserOrigin.Twitter, videosRegistered.internalVideoIDs(0), VideoOrigin.YouTube, 1)
    val twitterUserAssociation = UserVideoImplicitAssociation(twitterUserRegistered.internalUserID + 2,
      UserOrigin.Twitter,
      twitterUserRegistered.internalUserID,
      UserOrigin.Twitter,
      videosRegistered.internalVideoIDs(0),
      VideoOrigin.YouTube)
    val wykopUserVideoRating =
      UserVideoRating(wykopUserRegistered.internalUserID, UserOrigin.Wykop, videosRegistered.internalVideoIDs(1), VideoOrigin.Vimeo, 2)
    val wykopUserAssociation = UserVideoImplicitAssociation(twitterUserRegistered.internalUserID + 3,
      UserOrigin.Wykop,
      wykopUserRegistered.internalUserID,
      UserOrigin.Wykop,
      videosRegistered.internalVideoIDs(1),
      VideoOrigin.Vimeo)
    
    cassandraClient.updateRating(twitterUserVideoRating)
    cassandraClient.updateRating(wykopUserVideoRating)

    when(twitterAPI.followersFor(1L, FollowersBuilder(TwitterFollowersAPI.InitialCursor)))
      .thenReturn(FollowersBuilder(TwitterFollowersAPI.FinalCursorValue, Vector(TwitterUser(2, "second-twitter-user")), false)) 
    when(wykopAPI.usersFollowingUser("wykop-user", FollowersBuilder[String](WykopAPI.InitialPage)))
      .thenReturn(FollowersBuilder[String](WykopAPI.FinalPage, Vector("user-following-wykop-user"), false))
    
    // when
    producer.requestRelationshipsForTwitterUser(registerFirstTwitterUser.externalUserID)
    producer.requestRelationshipsForWykopUser(registerFirstWykopUser.externalUserName)
    Thread.sleep(10.seconds.toMillis)

    // then
    consumer.shutdown()
    val associations = userVideoRatingRepository.allAssociations()
    val twitterAssociation = 
      associations.filter(association => association.userOrigin == UserOrigin.Twitter && association.followedUserOrigin == UserOrigin.Twitter).head
    val wykopAssociation =
      associations.filter(association => association.userOrigin == UserOrigin.Wykop && association.followedUserOrigin == UserOrigin.Wykop).head
    
    assert(userVideoRatingRepository.allRatings().toSet == Set(twitterUserVideoRating, wykopUserVideoRating))
    assert(userVideoRatingRepository.allAssociations().size == 2)
    assert(twitterAssociation.followedInternalUserID == twitterUserVideoRating.userID)
    assert(wykopAssociation.followedInternalUserID == wykopUserVideoRating.userID)
    assert(twitterAssociation.internalUserID != wykopAssociation.internalUserID)
  }

  override def beforeAll(): Unit = {
    GlobalGraphOperations.cleanDatabase(graphDb)
    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer, metadataRepository)
    KafkaUtils.createTopic(fetcherConfig.getString("consumer.zookeeper.connect"), twitterTestTopic)
    KafkaUtils.createTopic(fetcherConfig.getString("consumer.zookeeper.connect"), wykopTestTopic)
  }

  override def afterAll(): Unit = {
    logger.info(s"Deleting topic $twitterTestTopic")
    
    KafkaUtils.removeTopic(fetcherConfig.getString("consumer.zookeeper.connect"), twitterTestTopic)
    KafkaUtils.removeTopic(fetcherConfig.getString("consumer.zookeeper.connect"), wykopTestTopic)
    /*stopping only sparkContext and not Cassandra because CassandraSpec shuts it down
    and it sometimes causes problems 
     */
    sparkCassandraClient.sparkContext.stop()
    super.afterAll()
  }
}
