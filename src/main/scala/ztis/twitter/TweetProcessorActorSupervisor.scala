package ztis.twitter

import akka.actor.SupervisorStrategy._
import akka.actor._
import ztis.cassandra.CassandraClient
import ztis.relationships.RelationshipFetcherProducer

import scala.concurrent.duration._

object TweetProcessorActorSupervisor {

  def props(cassandraClient: CassandraClient,
            userServiceActor: ActorRef,
            videoServiceActor: ActorRef,
            relationshipFetcherProducer: RelationshipFetcherProducer): Props = {
    Props(classOf[TweetProcessorActorSupervisor], cassandraClient, userServiceActor, videoServiceActor, relationshipFetcherProducer)
  }
}

class TweetProcessorActorSupervisor(cassandraClient: CassandraClient,
                                    userServiceActor: ActorRef,
                                    videoServiceActor: ActorRef,
                                    relationshipFetcherProducer: RelationshipFetcherProducer) extends Actor with ActorLogging {

  private val timeout: FiniteDuration = context.system.settings.config.getInt("tweet-processor.tweet-timeout-seconds").seconds

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 0) {
    case e: Exception => {
      log.error(e, "Encountered problem when processing a Tweet")
      Stop
    }
  }

  override def receive: Receive = {
    case tweet: Tweet => {
      context.actorOf(TweetProcessorActor.props(tweet, timeout, cassandraClient, userServiceActor = userServiceActor, videoServiceActor = videoServiceActor, relationshipFetcherProducer))
    }
  }
}
