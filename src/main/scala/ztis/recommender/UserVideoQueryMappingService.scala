package ztis.recommender

import akka.pattern.ask
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.user_video_service.VideoServiceActor
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.util.Timeout
import ztis.user_video_service.UserVideoServiceQueryActor.{VideoExternalInformationResponse, VideoExternalInformationRequest, UserInternalIDResponse, UserInternalIDRequest}

import scala.util.{Failure, Success, Try}

class UserVideoQueryMappingService(userVideoQueryService: ActorRef) extends MappingService with StrictLogging {
  implicit private val timeout = Timeout(5.seconds)

  override def identifyUser(twitterName: String, wykopName: String): (Option[Int], Option[Int]) = {
    val future = (userVideoQueryService ? UserInternalIDRequest(
      twitterUserName = toOption(twitterName), wykopUserName = toOption(wykopName))).mapTo[UserInternalIDResponse]
    val futureResponse = Try( Await.result(future, timeout.duration) )

    futureResponse match {
      case Success(response) => (response.internalTwitterUserID, response.internalWykopUserID)
      case Failure(exception) => {
        logger.error("Problem with a identify user request to user-video-query-service", exception)
        (None, None)
      }
    }
  }

  override def resolveVideo(videoId: Int): Option[Video] = {
    val future = (userVideoQueryService ? VideoExternalInformationRequest(videoId)).mapTo[VideoExternalInformationResponse]
    val futureResponse = Try( Await.result(future, timeout.duration) )

    futureResponse match {
      case Success(response) => toRecommenderVideo(response.video)
      case Failure(exception) => {
        logger.error("Problem with a resolve video request to user-video-query-service", exception)
        None
      }
    }
  }

  private def toOption(username: String): Option[String] = if (username.isEmpty) None else Some(username)

  private def toRecommenderVideo(maybeVideo: Option[VideoServiceActor.Video]) : Option[Video] = {
    maybeVideo.map(video => Video(video.uri.toURL.toString))
  }
}
