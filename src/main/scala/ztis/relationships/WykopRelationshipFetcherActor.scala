package ztis.relationships

import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive
import ztis.UserOrigin
import ztis.cassandra.SparkCassandraClient
import ztis.relationships.WykopRelationshipFetcherActor.FetchRelationshipsWykop
import ztis.user_video_service.UserServiceActor.{CreateRelationshipsToWykopUser, ToWykopUserRelationshipsCreated}
import ztis.wykop.WykopAPI

object WykopRelationshipFetcherActor {

  case class FetchRelationshipsWykop(id: String)

  case object WykopRateReset

  def props(wykopAPI: WykopAPI,
            userServiceActor: ActorRef,
            sparkCassandra: SparkCassandraClient): Props = {
    Props(classOf[WykopRelationshipFetcherActor], wykopAPI, userServiceActor, sparkCassandra)
  }
}

class WykopRelationshipFetcherActor(wykopAPI: WykopAPI,
                                    userServiceActor: ActorRef,
                                    sparkCassandra: SparkCassandraClient) extends RelationshipFetcher {

  override def receive: Receive = LoggingReceive {
    case FetchRelationshipsWykop(userName) => {
      try {
        val usersFollowingUser = wykopAPI.usersFollowingUser(userName)

        userServiceActor ! CreateRelationshipsToWykopUser(userName, usersFollowingUser)
      } catch {
        case e: Exception => {
          log.error(e, s"Error when fetching relationships for Wykop user $userName")
        }
      }
    }
    case ToWykopUserRelationshipsCreated(internalUserID: Int, fromUsers: Vector[Int]) => {
      sparkJobForUpdating(sparkCassandra, internalUserID, UserOrigin.Wykop, fromUsers)
    }
  }
}
