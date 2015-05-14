package ztis.twitter

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.routing.FromConfig
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.cassandra.{CassandraClient, CassandraConfiguration}

object TweetProcessorApp extends App with StrictLogging {
  val config = ConfigFactory.load("tweet-processor")
  val system = ActorSystem("ClusterSystem", config)
  val cassandraConfig = CassandraConfiguration(config)
  val cassandraClient = new CassandraClient(cassandraConfig)
  val extractor = null // new TwitterUserAndRatingExtractor(config, cassandraClient)

  val cluster = Cluster(system).registerOnMemberUp {
    logger.info("Creating TweetProcessor")
    val userServiceRouter = createRouter("user-service-router")
    val videoServiceRouter = createRouter("video-service-router")

    new TweetProcessor(system, config, cassandraClient, userServiceActor = userServiceRouter, videoServiceActor = videoServiceRouter)
  }

  def createRouter(routerName: String) = {
    system.actorOf(FromConfig.props(), name = routerName)
  }
}
