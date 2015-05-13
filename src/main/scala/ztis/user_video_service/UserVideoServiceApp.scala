package ztis.user_video_service

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import ztis.user_video_service.persistence.{GlobalGraphOperations, MetadataRepository, SchemaInitializer, UserRepository}

object UserVideoServiceApp extends App with StrictLogging {
  val config = ConfigFactory.load("user-video-service")
  val system = ActorSystem("ClusterSystem", config)

  val graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(config.getString("neo4j.path"))
  val schemaInitializer = new SchemaInitializer(graphDb)
  val userRepository = new UserRepository(graphDb)
  val metadataRepository = new MetadataRepository(graphDb)

  GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer)

  val cluster = Cluster(system).registerOnMemberUp {
    logger.info("Starting actor for servicing user requests")

    system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository), "user-service-actor")
  }
}
