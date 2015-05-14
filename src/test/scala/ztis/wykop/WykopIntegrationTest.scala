package ztis.wykop

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import ztis.cassandra.{CassandraClient, CassandraConfiguration, UserAndRatingRepository}
import ztis.user_video_service.persistence._
import ztis.user_video_service.{UserServiceActor, VideoServiceActor}
import ztis.{UserAndRating, UserOrigin, VideoOrigin}

import scala.concurrent.duration._

class WykopIntegrationTest extends FlatSpec with BeforeAndAfterAll with MockitoSugar {

  val mainPageEntries = Vector(EntriesFixture.firstVideoEntry)
  val upcomingPageEntries = Vector(EntriesFixture.secondVideoEntry, EntriesFixture.nonVideoEntry)
  val config = ConfigFactory.load("wykop")
  val cassandraConfig = CassandraConfiguration(config)

  val system = ActorSystem("TestSystem", config)
  val graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase()
  val metadataRepository = new MetadataRepository(graphDb)
  val schemaInitializer = new SchemaInitializer(graphDb, Option(10.seconds))
  val userRepository = new UserRepository(graphDb)
  val videoRepository = new VideoRepository(graphDb)
  val cassandraClient = new CassandraClient(cassandraConfig)
  val repository = new UserAndRatingRepository(cassandraClient)

  val api: WykopAPI = mock[WykopAPI]
  when(api.mainPageEntries(1)).thenReturn(mainPageEntries).thenReturn(Vector())
  when(api.upcomingPageEntries(1)).thenReturn(upcomingPageEntries).thenReturn(Vector())

  "Scrapped entries from wykop" should "be processed and presisted in cassandra" in {
    val userService = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))
    val videoService = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
    val scrapperActor = system.actorOf(WykopScrapperActor.props(api, cassandraClient, userServiceActor = userService, videoServiceActor = videoService))

    Thread.sleep(10.seconds.toMillis)
    val ratings = repository.allRatings().toSet
    val expectedRatings = Set(UserAndRating(0, UserOrigin.Wykop, 0, VideoOrigin.YouTube, 1, 0),
      UserAndRating(1, UserOrigin.Wykop, 1, VideoOrigin.Vimeo, 1, 0))
    assert(ratings == expectedRatings)
  }

  override def beforeAll(): Unit = {
    GlobalGraphOperations.cleanDatabase(graphDb)
    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer, metadataRepository)
  }

  override def afterAll(): Unit = {
    cassandraClient.clean()
    system.shutdown()
  }
}
