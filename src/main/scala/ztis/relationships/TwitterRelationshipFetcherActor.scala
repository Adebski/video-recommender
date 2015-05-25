package ztis.relationships

import akka.actor._
import akka.event.LoggingReceive
import ztis.UserOrigin
import ztis.cassandra.SparkCassandraClient
import ztis.relationships.TwitterRelationshipFetcherActor.{FetchRelationshipsTwitter, TwitterRateReset}
import ztis.twitter.TwitterUser
import ztis.user_video_service.UserServiceActor.{CreateRelationshipsToTwitterUser, ToTwitterUserRelationshipsCreated}

object TwitterRelationshipFetcherActor {

  case class FetchRelationshipsTwitter(id: Long)

  case object TwitterRateReset

  def props(twitterAPI: TwitterFollowersAPI,
            userServiceActor: ActorRef,
            sparkCassandra: SparkCassandraClient): Props = {
    Props(classOf[TwitterRelationshipFetcherActor], twitterAPI, userServiceActor, sparkCassandra)
  }
}

class TwitterRelationshipFetcherActor(twitterAPI: TwitterFollowersAPI,
                                      userServiceActor: ActorRef,
                                      sparkCassandra: SparkCassandraClient) extends RelationshipFetcher {

  private var twitterRateExceeded = false

  private var followersBuilder: Option[FollowersBuilder[TwitterUser]] = None

  override def receive: Receive = LoggingReceive {
    case FetchRelationshipsTwitter(userID) => {
      try {
        if (twitterRateExceeded) {
          stash()
        } else {
          val partialyFilledBuilder = getExistingBuilderOrCreateNewOne
          val fullyFilledBuilder = twitterAPI.followersFor(userID, partialyFilledBuilder)
          userServiceActor ! CreateRelationshipsToTwitterUser(userID, fullyFilledBuilder.gatheredFollowers)
          followersBuilder = None
        }
      } catch {
        case e: TwitterFollowersFetchingLimitException => {
          twitterRateExceeded = true
          followersBuilder = Some(e.partialBuilder)
          scheduleMessageAndStashCurrent(TwitterRateReset, e.reason.getRateLimitStatus.getSecondsUntilReset)
        }
        case e: Exception => {
          log.error(e, s"Error when fetching relationships for Twitter user $userID")
          followersBuilder = None
        }
      }
    }
    case ToTwitterUserRelationshipsCreated(internalUserID: Int, fromUsers: Vector[Int]) => {
      sparkJobForUpdating(sparkCassandra, internalUserID, UserOrigin.Twitter, fromUsers)
    }
    case TwitterRateReset => {
      log.info("Restoring fetcher to work - rate limit has been restored")
      unstashAll()
      twitterRateExceeded = false
    }
  }

  private def getExistingBuilderOrCreateNewOne(): FollowersBuilder[TwitterUser] = {
    if (followersBuilder.isEmpty) {
      followersBuilder = Some(FollowersBuilder[TwitterUser](page = TwitterFollowersAPI.InitialCursor))
    }
    followersBuilder.get
  }
}
