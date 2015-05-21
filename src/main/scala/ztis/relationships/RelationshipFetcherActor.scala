package ztis.relationships

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingReceive
import ztis.UserOrigin
import ztis.cassandra.SparkCassandraClient
import ztis.relationships.RelationshipFetcherActor.{FetchRelationshipsTwitter, FetchRelationshipsWykop}
import ztis.user_video_service.UserServiceActor.{CreateRelationshipsToTwitterUser, CreateRelationshipsToWykopUser, ToTwitterUserRelationshipsCreated, ToWykopUserRelationshipsCreated}
import ztis.wykop.WykopAPI

import scala.concurrent.Future

object RelationshipFetcherActor {

  case class FetchRelationshipsTwitter(id: Long)

  case class FetchRelationshipsWykop(id: String)

  def props(twitterAPI: TwitterFollowersAPI,
            wykopAPI: WykopAPI,
            userServiceActor: ActorRef,
            sparkCassandra: SparkCassandraClient): Props = {
    Props(classOf[RelationshipFetcherActor], twitterAPI, wykopAPI, userServiceActor, sparkCassandra)
  }
}

class RelationshipFetcherActor(twitterAPI: TwitterFollowersAPI,
                               wykopAPI: WykopAPI,
                               userServiceActor: ActorRef,
                               sparkCassandra: SparkCassandraClient) extends Actor with ActorLogging {

  override def receive: Receive = LoggingReceive {
    case FetchRelationshipsTwitter(userID) => {
      try {
        val followers = twitterAPI.followersFor(userID)

        userServiceActor ! CreateRelationshipsToTwitterUser(userID, followers)
      } catch {
        case e: Exception => {
          log.error(e, s"Error when fetching relationships for Twitter user $userID")
        }
      }
    }
    case ToTwitterUserRelationshipsCreated(internalUserID: Int, fromUsers: Vector[Int]) => {
      sparkJobForUpdating(internalUserID, UserOrigin.Twitter, fromUsers)
    }
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
      sparkJobForUpdating(internalUserID, UserOrigin.Wykop, fromUsers)
    }
  }

  private def sparkJobForUpdating(internalUserID: Int, userOrigin: UserOrigin, fromUsers: Vector[Int]): Unit = {
    import context.dispatcher
    val f = Future {
      sparkCassandra.updateMoviesForNewRelationships(internalUserID, userOrigin, fromUsers, userOrigin)
    }
    f.onComplete {
      case scala.util.Success(_) => {
        log.info(s"Updated relationships, to user $internalUserID, from users $fromUsers, origin $userOrigin")
      }
      case scala.util.Failure(e) => {
        log.error(e, s"Error updating relationships, to user $internalUserID, from users $fromUsers, origin $userOrigin")
      }
    }
  }
}
