package ztis.relationships

import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive
import ztis.UserOrigin
import ztis.cassandra.SparkCassandraClient
import ztis.relationships.WykopRelationshipFetcherActor.{WykopRateReset, FetchRelationshipsWykop}
import ztis.user_video_service.UserServiceActor.{CreateRelationshipsToWykopUser, ToWykopUserRelationshipsCreated}
import ztis.wykop.{WykopFollowersFetchingLimitException, WykopAPI}

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

  private var wykopRateExceeded = false

  private var followersBuilder: Option[FollowersBuilder[String]] = None

  override def receive: Receive = LoggingReceive {
    case FetchRelationshipsWykop(userName) => {
      try {
        if (wykopRateExceeded) {
          stash()
        } else {
          val partialyFilledBuilder = getExistingBuilderOrCreateNewOne
          val fullyFilledBuilder = wykopAPI.usersFollowingUser(userName, partialyFilledBuilder)

          userServiceActor ! CreateRelationshipsToWykopUser(userName, fullyFilledBuilder.gatheredFollowers)  
          followersBuilder = None
        }
      } catch {
        case e: WykopFollowersFetchingLimitException => {
          
          val secondsToWait = e.waitFor.toSeconds
          wykopRateExceeded = true
          followersBuilder = Some(e.partialBuilder)
          scheduleMessageAndStashCurrent(WykopRelationshipFetcherActor.WykopRateReset, secondsToWait)
        }
        case e: Exception => {
          log.error(e, s"Error when fetching relationships for Wykop user $userName")
        }
      }
    }
    case ToWykopUserRelationshipsCreated(internalUserID: Int, fromUsers: Vector[Int]) => {
      sparkJobForUpdating(sparkCassandra, internalUserID, UserOrigin.Wykop, fromUsers)
    }
    case WykopRateReset => {
      log.info("Restoring fetcher to work - rate limit has been restored")
      unstashAll()
      wykopRateExceeded = false
    }
  }

  private def getExistingBuilderOrCreateNewOne(): FollowersBuilder[String] = {
    if (followersBuilder.isEmpty) {
      followersBuilder = Some(FollowersBuilder[String](page = WykopAPI.InitialPage))
    }
    followersBuilder.get
  }
}
