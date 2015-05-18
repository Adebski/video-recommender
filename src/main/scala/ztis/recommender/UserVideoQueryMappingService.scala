package ztis.recommender

import akka.pattern.ask
import ztis.user_video_service.VideoServiceActor
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.util.Timeout
import ztis.user_video_service.UserVideoServiceQueryActor.{VideoExternalInformationResponse, VideoExternalInformationRequest, UserInternalIDResponse, UserInternalIDRequest}

class UserVideoQueryMappingService(userVideoQueryService: ActorRef) extends MappingService {
  implicit private val timeout = Timeout(5.seconds)

  override def identifyUser(twitterName: String, wykopName: String): (Option[Int], Option[Int]) = {
    val future = (userVideoQueryService ? UserInternalIDRequest(
      twitterUserName = toOption(twitterName), wykopUserName = toOption(wykopName))).mapTo[UserInternalIDResponse]
    val response = Await.result(future, timeout.duration)

    (response.internalTwitterUserID, response.internalWykopUserID)
  }

  override def resolveVideo(videoId: Int): Option[Video] = {
    val future = (userVideoQueryService ? VideoExternalInformationRequest(videoId)).mapTo[VideoExternalInformationResponse]
    val response = Await.result(future, timeout.duration)

    toRecommenderVideo(response.video)
  }

  private def toOption(username: String): Option[String] = if (username.isEmpty) None else Some(username)

  private def toRecommenderVideo(maybeVideo: Option[VideoServiceActor.Video]) : Option[Video] = {
    maybeVideo.map(video => Video(video.uri.toURL.toString))
  }
}
