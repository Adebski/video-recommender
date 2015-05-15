package ztis.wykop

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.routing.FromConfig
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.cassandra.{CassandraClient, CassandraConfiguration}

object WykopStreamApp extends App with StrictLogging {
  val config = ConfigFactory.load("wykop")
  val system = ActorSystem("ClusterSystem", config)
  val api = new WykopAPI(config)
  val cassandraConfig = CassandraConfiguration(config)
  val cassandraClient = new CassandraClient(cassandraConfig)

  val cluster = Cluster(system).registerOnMemberUp {
    logger.info("Creating WykopScrapperActor")
    val userServiceRouter = createRouter("user-service-router")
    val videoServiceRouter = createRouter("video-service-router")
    val wykopScrapperActor =
      system.actorOf(WykopScrapperActor.props(api, cassandraClient, userServiceActor = userServiceRouter, videoServiceActor = videoServiceRouter), "wykop-scrapper-actor")
  }
  

  def createRouter(routerName: String) = {
    system.actorOf(FromConfig.props(), name = routerName)
  }
}
