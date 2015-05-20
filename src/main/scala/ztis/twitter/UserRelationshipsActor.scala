package ztis.twitter

import akka.actor.Status.Failure
import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import akka.event.LoggingReceive
import ztis.UserOrigin
import ztis.cassandra.SparkCassandraClient
import ztis.user_video_service.UserServiceActor.{CreateRelationshipsToTwitterUser, ToTwitterUserRelationshipsCreated}

object UserRelationshipsActor {
  def props(userID: Long,
            fromUsers: Vector[TwitterUser],
            userServiceActor: ActorRef,
            sparkCassandraClient: SparkCassandraClient): Props = {
    Props(classOf[UserRelationshipsActor], userID, fromUsers, userServiceActor, sparkCassandraClient)
  }
}

class UserRelationshipsActor(userID: Long,
                             fromUsers: Vector[TwitterUser],
                             userServiceActor: ActorRef,
                             sparkCassandraClient: SparkCassandraClient) extends Actor with ActorLogging {

  userServiceActor ! CreateRelationshipsToTwitterUser(userID, fromUsers)

  override def receive: Receive = LoggingReceive {
    case response: ToTwitterUserRelationshipsCreated => {
      sparkCassandraClient.updateMoviesForNewRelationships(response.internalUserID, UserOrigin.Twitter, response.fromUsers, UserOrigin.Twitter)
      context.stop(self)
    }
    case Failure(e) => {
      log.error(e, s"Could not process relationships for userID = $userID and fromUsers = $fromUsers")
      context.stop(self)
    }
  }
}
