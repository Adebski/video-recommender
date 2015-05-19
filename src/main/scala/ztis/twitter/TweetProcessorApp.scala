package ztis.twitter

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.cassandra.{CassandraClient, CassandraConfiguration}
import ztis.util.RouterSupport

object TweetProcessorApp extends App with StrictLogging with RouterSupport {
  val config = ConfigFactory.load("tweet-processor")
  val relationshipFetcherConfig = ConfigFactory.load("twitter-relationship-fetcher")
  val system = ActorSystem("ClusterSystem", config)
  val cassandraConfig = CassandraConfiguration(config)
  val cassandraClient = new CassandraClient(cassandraConfig)
  val relationshipFetcherProducer = new RelationshipFetcherProducer(relationshipFetcherConfig)

  val cluster = Cluster(system).registerOnMemberUp {
    logger.info("Creating TweetProcessor")
    val userServiceRouter = createUserServiceRouter(system)
    val videoServiceRouter = createVideoServiceRouter(system)
    val tweetProcessorActor =
      system.actorOf(TweetProcessorActorSupervisor.props(cassandraClient, userServiceActor = userServiceRouter, videoServiceActor = videoServiceRouter, relationshipFetcherProducer), "tweet-processor-actor-supervisor")

    new TweetProcessor(system, config, tweetProcessorActor)
  }
}
