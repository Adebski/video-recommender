package ztis.wykop

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import ztis.cassandra.{CassandraClient, CassandraConfiguration, UserVideoRatingRepository}
import ztis.relationships.{RelationshipFetcherProducer, KafkaRelationshipFetcherProducer}
import ztis.user_video_service.persistence._
import ztis.user_video_service.{UserServiceActor, VideoServiceActor}
import ztis.{UserVideoRating, UserOrigin, VideoOrigin}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

/*
This test may not pass due to timing. The thing is that we are creating separate actor for processing each entry 
Because we have one entry from main page and second one from upcoming page it is possible that 
the entry from upcoming page will be processed before entry from main page.
 */
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
  val repository = new UserVideoRatingRepository(cassandraClient)
  val fetcherProducer = new RelationshipFetcherProducerStub
  
  val api: WykopAPI = mock[WykopAPI]
  when(api.mainPageEntries(1)).thenReturn(mainPageEntries).thenReturn(Vector())
  when(api.upcomingPageEntries(1)).thenReturn(upcomingPageEntries).thenReturn(Vector())
  
  "Scrapped entries from wykop" should "be processed and presisted in cassandra" in {
    val userService = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))
    val videoService = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
    val scrapperActor = system.actorOf(WykopScrapperActor.props(api, cassandraClient, userServiceActor = userService, videoServiceActor = videoService, fetcherProducer))

    Thread.sleep(10.seconds.toMillis)
    val ratings = repository.allRatings().toSet
    val expectedRatings = Set(UserVideoRating(0, UserOrigin.Wykop, 0, VideoOrigin.YouTube, 1),
      UserVideoRating(1, UserOrigin.Wykop, 1, VideoOrigin.Vimeo, 1))
    assert(ratings == expectedRatings)
    val expectedQueuedUsers = Set(
      EntriesFixture.firstVideoEntry.author,
      EntriesFixture.secondVideoEntry.author
    )
    assert(fetcherProducer.wykopUsers.toSet == expectedQueuedUsers)
  }

  override def beforeAll(): Unit = {
    GlobalGraphOperations.cleanDatabase(graphDb)
    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer, metadataRepository)
  }

  override def afterAll(): Unit = {
    cassandraClient.clean()
    cassandraClient.shutdown()
    system.shutdown()
  }
}

class RelationshipFetcherProducerStub extends RelationshipFetcherProducer {
  
  val wykopUsers = ArrayBuffer.empty[String]
  
  val twitterUsers = ArrayBuffer.empty[Long]
  
  override def requestRelationshipsForWykopUser(wykopUserID: String): Unit = {
    wykopUsers.synchronized {
      wykopUsers += wykopUserID  
    }
  }

  override def requestRelationshipsForTwitterUser(twitterUserID: Long): Unit = {
    twitterUsers.synchronized {
      twitterUsers += twitterUserID
    }
  }
}
