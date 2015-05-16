package ztis.twitter

import akka.actor.SupervisorStrategy._
import akka.actor._
import ztis.cassandra.CassandraClient

import scala.concurrent.duration._

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

  private val timeout: FiniteDuration = context.system.settings.config.getInt("tweet-processor.tweet-timeout-seconds").seconds
  
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 0) {
    case e: Exception => {
      log.error(e, "Encountered problem when processing a Tweet")
      Stop
    }
  }

  override def receive: Receive = {
    case tweet: Tweet => {
      context.actorOf(TweetProcessorActor.props(tweet, timeout, cassandraClient, userServiceActor = userServiceActor, videoServiceActor = videoServiceActor))
    }
  }
}
