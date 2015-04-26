package ztis.recommender

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

object Recommender extends App with StrictLogging {
  val config = ConfigFactory.load("recommender")

  val interface = config.getString("recommender.interface")
  val port = config.getInt("recommender.port")

  implicit val system = ActorSystem("on-spray-can", config)

  val service = system.actorOf(Props[RecommenderPortActor], "demo-service")

  implicit val timeout = Timeout(5.seconds)
  IO(Http) ? Http.Bind(service, interface, port)

  logger.info("Recommender service started.")
}
