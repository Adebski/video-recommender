package ztis.recommender

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.io.IO
import akka.pattern.ask
import akka.routing.FromConfig
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import spray.can.Http

import scala.concurrent.duration._

object Recommender extends App with StrictLogging {
  val config = ConfigFactory.load("recommender")

  val interface = config.getString("recommender.interface")
  val port = config.getInt("recommender.port")

  implicit val system = ActorSystem("ClusterSystem", config)

  Cluster(system).registerOnMemberUp {
    val userServiceRouter = createRouter("user-service-router")
    val videoServiceRouter = createRouter("video-service-router")

    val service = system.actorOf(RecommenderPortActor.props(userServiceRouter, videoServiceRouter), "recommender-service")

    implicit val timeout = Timeout(5.seconds)
    IO(Http) ? Http.Bind(service, interface, port)

    logger.info("Recommender service started.")
  }

  private def createRouter(routerName: String) = {
    system.actorOf(FromConfig.props(), name = routerName)
  }
}
