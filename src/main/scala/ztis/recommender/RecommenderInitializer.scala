package ztis.recommender

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import spray.can.Http
import ztis.Initializer

import scala.concurrent.duration._

class RecommenderInitializer extends Initializer {
  override def initialize(): Unit = {
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
}
