package ztis.twitter

import akka.actor.SupervisorStrategy._
import akka.actor._
import ztis.cassandra.CassandraClient

object TweetProcessorActorSupervisor {

  def props(cassandraClient: CassandraClient,
            userServiceActor: ActorRef,
            videoServiceActor: ActorRef): Props = {
    Props(classOf[TweetProcessorActorSupervisor], cassandraClient, userServiceActor, videoServiceActor)
  }
}

class TweetProcessorActorSupervisor(cassandraClient: CassandraClient,
                                    userServiceActor: ActorRef,
                                    videoServiceActor: ActorRef) extends Actor with ActorLogging {

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 0) {
    case e: Exception => {
      log.error(e, "Encountered problem when processing a Tweet")
      Stop
    }
  }

  override def receive: Receive = {
    case tweet: Tweet => {
      context.actorOf(TweetProcessorActor.props(tweet, cassandraClient, userServiceActor = userServiceActor, videoServiceActor = videoServiceActor))
    }
  }
}
