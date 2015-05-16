package ztis.user_video_service

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Props}
import akka.event.LoggingReceive
import org.neo4j.graphdb.GraphDatabaseService
import ztis.VideoOrigin
import ztis.user_video_service.ServiceActorMessages.{NextInternalIDRequest, NextInternalIDResponse}
import ztis.user_video_service.VideoServiceActor.{VideosRegistered, RegisterVideos}
import ztis.user_video_service.persistence.{Metadata, MetadataRepository, UnitOfWork, VideoRepository}

object VideoServiceActor {

  case class Video(origin: VideoOrigin, uri: java.net.URI)
  
  case class RegisterVideos(videos: Vector[Video])

  case class VideosRegistered(internalVideoIDs: Vector[Int], request: RegisterVideos)

  def props(graphDatabaseService: GraphDatabaseService,
            videoRepository: VideoRepository,
            metadataRepository: MetadataRepository): Props = {
    Props(classOf[VideoServiceActor], graphDatabaseService, videoRepository, metadataRepository)
  }
}

class VideoServiceActor(graphDatabaseService: GraphDatabaseService,
                        videoRepository: VideoRepository,
                        metadataRepository: MetadataRepository) 
  extends Actor with UnitOfWork with ActorLogging {

  private implicit val _service = graphDatabaseService

  private var nextInternalID: Int = fetchMetadata.nextVideoInternalID

  private var tempNextInternalID: Int = nextInternalID

  private def fetchMetadata: Metadata = {
    unitOfWork { () =>
      metadataRepository.metadata
    }
  }
  
  override def receive: Receive = LoggingReceive {
    case NextInternalIDRequest => {
      sender() ! NextInternalIDResponse(nextInternalID)
    }

    case request: RegisterVideos => {
      try {
        val response: VideosRegistered = unitOfWork { () => 
          val result: (Int, VideosRegistered) = videoRepository.getOrCreateVideos(request, tempNextInternalID)

          tempNextInternalID = result._1
          metadataRepository.updateNextVideoInternalID(tempNextInternalID)

          result._2
        }

        sender() ! response
      } catch {
        case e: Exception => {
          log.error(e, s"Something went wrong during processing of $request")
          /*
          We are rolling back tempNextInternalID to the one that was present before we started to process given request
           */
          tempNextInternalID = nextInternalID
          sender() ! Failure(e)
        }
      }
      nextInternalID = tempNextInternalID
    }
  }
}
