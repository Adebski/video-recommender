package ztis.user_video_service

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import ztis.Initializer
import ztis.user_video_service.persistence._

class UserVideoServiceInitializer extends Initializer {
  override def initialize(): Unit = {
    val config = ConfigFactory.load("user-video-service")
    val system = ActorSystem("ClusterSystem", config)

    val graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(config.getString("neo4j.path"))
    val schemaInitializer = new SchemaInitializer(graphDb)
    val userRepository = new UserRepository(graphDb)
    val videoRepository = new VideoRepository(graphDb)
    val metadataRepository = new MetadataRepository(graphDb)

    GlobalGraphOperations.initializeDatabase(graphDb, schemaInitializer, metadataRepository)

    Cluster(system).registerOnMemberUp {
      logger.info("Starting actor for servicing user requests")
      system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository).withDispatcher("service-actors-dispatcher"), "user-service-actor")
      logger.info("Starting actor for servicing video requests")
      system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository).withDispatcher("service-actors-dispatcher"), "video-service-actor")
      logger.info("Starting actor that responds to internal id queries")
      system.actorOf(UserVideoServiceQueryActor.props(graphDb, userRepository, videoRepository).withDispatcher("service-actors-dispatcher"), "user-video-service-query-actor")
    }
  }
}
