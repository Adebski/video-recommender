package ztis.util

import akka.actor.{ActorRef, ActorSystem}
import akka.routing.FromConfig

trait RouterSupport {
  def createUserServiceRouter(system: ActorSystem): ActorRef = {
    createRouter(system, "user-service-router")    
  }

  def createVideoServiceRouter(system: ActorSystem): ActorRef = {
    createRouter(system, "video-service-router")
  }
  
  def createUserVideoServiceQueryRouter(system: ActorSystem): ActorRef = {
    createRouter(system, "user-video-service-query-router")
  }
  
  private def createRouter(system: ActorSystem, routerName: String) = {
    system.actorOf(FromConfig.props(), name = routerName)
  }
}
