package ztis.twitter

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import ztis.Initializer
import ztis.cassandra.{CassandraClient, CassandraConfiguration}
import ztis.relationships.KafkaRelationshipFetcherProducer

class TweetProcessorInitializer extends Initializer {
  override def initialize(): Unit = {
    val config = ConfigFactory.load("tweet-processor")
    val relationshipFetcherConfig = ConfigFactory.load("relationship-fetcher")
    val system = ActorSystem("ClusterSystem", config)
    val cassandraConfig = CassandraConfiguration(config)
    val cassandraClient = new CassandraClient(cassandraConfig)
    val relationshipFetcherProducer = new KafkaRelationshipFetcherProducer(relationshipFetcherConfig)

    Cluster(system).registerOnMemberUp {
      logger.info("Creating TweetProcessor")
      val userServiceRouter = createUserServiceRouter(system)
      val videoServiceRouter = createVideoServiceRouter(system)
      val tweetProcessorActor =
        system.actorOf(TweetProcessorActorSupervisor.props(cassandraClient, userServiceActor = userServiceRouter, videoServiceActor = videoServiceRouter, relationshipFetcherProducer), "tweet-processor-actor-supervisor")

      new TweetProcessor(system, config, tweetProcessorActor)
    }
  }
}
