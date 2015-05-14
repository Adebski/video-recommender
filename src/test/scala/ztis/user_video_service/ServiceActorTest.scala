package ztis.user_video_service

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.{BeforeAndAfter, WordSpecLike}
import ztis.user_video_service.persistence.{GlobalGraphOperations, MetadataRepository, SchemaInitializer}

import scala.concurrent.duration._

abstract class ServiceActorTest
  extends TestKit(ActorSystem("test-system", ConfigFactory.load("akka"))) with ImplicitSender with WordSpecLike with BeforeAndAfter {

  val graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
  val metadataRepository = new MetadataRepository(graphDb)
  val schemaInitializer = new SchemaInitializer(graphDb, Option(10.seconds))


  before {
    GlobalGraphOperations.cleanDatabase(graphDb)
    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer)
  }
}
