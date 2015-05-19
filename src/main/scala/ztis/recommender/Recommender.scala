package ztis.recommender

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import spray.can.Http
import ztis.util.RouterSupport

import scala.concurrent.duration._

object Recommender extends App with StrictLogging with RouterSupport {
  val config = ConfigFactory.load("recommender")

  val interface = config.getString("recommender.interface")
  val port = config.getInt("recommender.port")

  implicit val system = ActorSystem("ClusterSystem", config)

  Cluster(system).registerOnMemberUp {
    val userVideoServiceRouter = createUserVideoServiceQueryRouter(system)

    val service = system.actorOf(RecommenderPortActor.props(userVideoServiceRouter), "recommender-service")

    implicit val timeout = Timeout(5.seconds)
    IO(Http) ? Http.Bind(service, interface, port)

    logger.info("Recommender service started.")
  }
}
