package ztis.wykop

import akka.actor._
import akka.event.LoggingReceive
import ztis.cassandra.CassandraClient
import ztis.user_video_service.UserServiceActor.{RegisterWykopUser, WykopUserRegistered}
import ztis.user_video_service.VideoServiceActor.{RegisterVideos, VideosRegistered}
import ztis.wykop.VideoEntryProcessorActor.{Timeout, ProcessEntry}
import ztis.{UserVideoRating, UserOrigin}

import scala.concurrent.duration.FiniteDuration

object VideoEntryProcessorActor {

  private case class ProcessEntry(videoEntry: VideoEntry)

  private case object Timeout
  
  def props(entry: VideoEntry,
            timeout: FiniteDuration,
            cassandraClient: CassandraClient, 
            userServiceActor: ActorRef, 
            videoServiceActor: ActorRef): Props = {
    Props(classOf[VideoEntryProcessorActor], entry, timeout, cassandraClient, userServiceActor, videoServiceActor)
  }
}

class VideoEntryProcessorActor(entry: VideoEntry,
                               timeout: FiniteDuration,
                               cassandraClient: CassandraClient,
                               userServiceActor: ActorRef,
                               videoServiceActor: ActorRef) extends Actor with ActorLogging {

  self ! ProcessEntry(entry)

  private var userResponse: Option[WykopUserRegistered] = None

  private var videoResponse: Option[VideosRegistered] = None

  private var timeoutMessage: Option[Cancellable] = None
  
  override def receive: Receive = LoggingReceive {
    case ProcessEntry(entry) => {
      userServiceActor ! RegisterWykopUser(entry.userName)
      videoServiceActor ! RegisterVideos(Vector(entry.video))
      scheduleTimeout()
    }
    case response: WykopUserRegistered => {
      userResponse = Some(response)

      if (bothResponsesReceived) {
        processResponses()
      }
    }
    case response: VideosRegistered => {
      videoResponse = Some(response)

      if (bothResponsesReceived) {
        processResponses()
      }
    }
    case Timeout => {
      log.warning(s"Timeout reached during processing $entry, userResponse = $userResponse, videoResponse = $videoResponse")
      context.stop(self)
    }
  }

  private def processResponses(): Unit = {
    try {
      val userID = userResponse.get.internalUserID
      val videoID = videoResponse.get.internalVideoIDs(0)
      val videoOrigin = entry.video.origin
      val toPersist =
        UserVideoRating(userID, UserOrigin.Wykop, videoID, videoOrigin, 1)
      log.info(s"Persisting $toPersist")

      cassandraClient.updateRating(toPersist)
    } catch {
      case e: Exception => {
        throw new IllegalArgumentException(s"Could not persist $userResponse and $videoResponse", e)
      }
    } finally {
      timeoutMessage.foreach(_.cancel())
      context.stop(self)
    }
  }

  private def bothResponsesReceived: Boolean = {
    userResponse.nonEmpty && videoResponse.nonEmpty
  }

  private def scheduleTimeout(): Unit = {
    import context.dispatcher
    timeoutMessage = Some(context.system.scheduler.scheduleOnce(timeout, self, Timeout))
  }
}
