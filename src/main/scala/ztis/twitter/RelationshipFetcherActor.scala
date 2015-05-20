package ztis.twitter

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.event.LoggingReceive
import ztis.cassandra.SparkCassandraClient
import ztis.twitter.RelationshipFetcherActor.FetchRelationships

object RelationshipFetcherActor {

  case class FetchRelationships(id: Long)

  def props(twitterAPI: FollowersAPI,
            userServiceActor: ActorRef,
            sparkCassandra: SparkCassandraClient): Props = {
    Props(classOf[RelationshipFetcherActor], twitterAPI, userServiceActor, sparkCassandra)
  }
}

class RelationshipFetcherActor(twitterAPI: FollowersAPI,
                               userServiceActor: ActorRef,
                               sparkCassandra: SparkCassandraClient) extends Actor with ActorLogging {

  override def receive: Receive = LoggingReceive {
    case FetchRelationships(userID) => {
      try {
        val followers = twitterAPI.followersFor(userID)
        context.actorOf(UserRelationshipsActor.props(userID, followers, userServiceActor, sparkCassandra))
      } catch {
        case e: Exception => {
          log.error(e, s"Error when fetching relationships for $userID")
        }
      }
    }
  }
}
