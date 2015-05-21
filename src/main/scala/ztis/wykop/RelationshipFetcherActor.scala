package ztis.wykop

import akka.actor.{Actor, ActorRef, Props}
import akka.event.LoggingReceive
import ztis.cassandra.SparkCassandraClient
import ztis.wykop.RelationshipFetcherActor.FetchRelationshipsFor

object RelationshipFetcherActor {

  case class FetchRelationshipsFor(userName: String)

  def props(api: WykopAPI,
            userServiceActor: ActorRef,
            sparkCassandraClient: SparkCassandraClient): Props = {
    Props(classOf[RelationshipFetcherActor], api, userServiceActor, sparkCassandraClient)
  }
}

class RelationshipFetcherActor(api: WykopAPI,
                               userServiceActor: ActorRef,
                               sparkCassandraClient: SparkCassandraClient) extends Actor {

  override def receive: Receive = LoggingReceive {
    case FetchRelationshipsFor(userName) => {
      val usersFollowingUser = api.usersFollowingUser(userName)

      context.actorOf(UserRelationshipsActor.props(userName, usersFollowingUser, userServiceActor, sparkCassandraClient))
    }
  }
}
