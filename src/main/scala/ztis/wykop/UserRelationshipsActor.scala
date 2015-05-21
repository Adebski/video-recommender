package ztis.wykop

import akka.actor.Status.Failure
import akka.actor.{ActorLogging, Actor, Props, ActorRef}
import akka.event.LoggingReceive
import ztis.UserOrigin
import ztis.cassandra.SparkCassandraClient
import ztis.user_video_service.UserServiceActor.{ToWykopUserRelationshipsCreated, CreateRelationshipsToWykopUser}

object UserRelationshipsActor {
  def props(userName: String,
            fromUsers: Vector[String],
            userServiceActor: ActorRef,
            sparkCassandraClient: SparkCassandraClient): Props = {
    Props(classOf[UserRelationshipsActor], userName, fromUsers, userServiceActor, sparkCassandraClient)
  }
}

class UserRelationshipsActor(userName: String,
                             fromUsers: Vector[String],
                             userServiceActor: ActorRef,
                             sparkCassandraClient: SparkCassandraClient) extends Actor with ActorLogging {

  userServiceActor ! CreateRelationshipsToWykopUser(userName, fromUsers)

  override def receive: Receive = LoggingReceive {
    case response: ToWykopUserRelationshipsCreated => {
      sparkCassandraClient.updateMoviesForNewRelationships(response.internalUserID, UserOrigin.Wykop, response.fromUsers, UserOrigin.Wykop)
      context.stop(self)
    }
    case Failure(e) => {
      log.error(e, s"Could not process relationships for userName = $userName and fromUsers = $fromUsers")
      context.stop(self)
    }
  }
}
