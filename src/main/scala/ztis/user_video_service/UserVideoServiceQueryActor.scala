package ztis.user_video_service

import akka.actor.Status.Failure
import akka.actor.{Props, ActorLogging, Actor}
import akka.event.LoggingReceive
import org.neo4j.graphdb.GraphDatabaseService
import ztis.user_video_service.UserVideoServiceQueryActor.{VideoExternalInformationResponse, VideoExternalInformationRequest, UserInternalIDResponse, UserInternalIDRequest}
import ztis.user_video_service.VideoServiceActor.Video
import ztis.user_video_service.persistence.{VideoRepository, UserRepository, UnitOfWork}

object UserVideoServiceQueryActor {
  case class UserInternalIDRequest(twitterUserName: Option[String], wykopUserName: Option[String])
  
  case class UserInternalIDResponse(internalTwitterUserID: Option[Int], internalWykopUserID: Option[Int])
  
  case class VideoExternalInformationRequest(internalVideoID: Int)
  
  case class VideoExternalInformationResponse(video: Option[Video])
  
  def props(graphDatabaseService: GraphDatabaseService,
            userRepository: UserRepository,
            videoRepository: VideoRepository): Props = {
    Props(classOf[UserVideoServiceQueryActor], graphDatabaseService, userRepository, videoRepository)
  }
}

class UserVideoServiceQueryActor(graphDatabaseService: GraphDatabaseService, 
                                 userRepository: UserRepository, 
                                 videoRepository: VideoRepository) extends Actor with ActorLogging with UnitOfWork {
  
  private implicit val _service = graphDatabaseService
  
  override def receive: Receive = LoggingReceive {
    case request: UserInternalIDRequest => {
      try {
        unitOfWork { () =>
          val twitterUserID: Option[Int] = request.twitterUserName.flatMap(userRepository.getTwitterUser(_))
          val wykopUserID: Option[Int] = request.wykopUserName.flatMap(userRepository.getWykopUser(_))
          sender() ! UserInternalIDResponse(internalTwitterUserID = twitterUserID, internalWykopUserID = wykopUserID)
        }  
      } catch {
        case e: Exception => {
          log.error(e, s"Error during processing $request")
          sender() ! Failure(e)
        }
      }
    }
    case request: VideoExternalInformationRequest => {
      unitOfWork { () =>
        sender() ! VideoExternalInformationResponse(videoRepository.getVideo(request))
      }  
    }
  }
}
