package ztis.user_video_service.persistence

import java.net.URI

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import ztis.VideoOrigin
import ztis.user_video_service.FieldNames
import ztis.user_video_service.UserVideoServiceQueryActor.VideoExternalInformationRequest
import ztis.user_video_service.VideoServiceActor.{Video, RegisterVideos, VideosRegistered}

class VideoRepository(graphDatabaseService: GraphDatabaseService) extends StrictLogging {
  implicit val _service = graphDatabaseService

  def getOrCreateVideos(request: RegisterVideos, nextInternalID: Int): (Int, VideosRegistered) = {
    var updatedNextInternalID = nextInternalID
    
    val videoIDs = request.videos.map { video =>
      val videoNode = getOrCreateVideo(video, updatedNextInternalID)
      val videoInternalID = videoNode.getProperty(FieldNames.InternalVideoID).asInstanceOf[Int]
      
      if (videoInternalID == updatedNextInternalID) {
        updatedNextInternalID += 1
      }
      
      videoInternalID
    }

    (updatedNextInternalID, VideosRegistered(videoIDs, request))
  }

  def getVideo(request: VideoExternalInformationRequest): Option[Video] = {
    val index = Indexes.VideoInternalID
    val node = Option(graphDatabaseService.findNode(index.label, index.property, request.internalVideoID))
    
    node.map(nodeToVideo)
  }
  
  private def nodeToVideo(node: Node): Video ={
    val origin = VideoOrigin.fromString(node.getProperty(FieldNames.VideoOrigin).asInstanceOf[String])
    val uri = URI.create(node.getProperty(FieldNames.VideoLink).asInstanceOf[String])
    
    Video(origin, uri)
  }
  
  private def getOrCreateVideo(video: Video, nextInternalID: Int): Node = {
    val node = Option(graphDatabaseService.findNode(Indexes.VideoLink.label, Indexes.VideoLink.property, video.uri.toString))
      .getOrElse(createNode(video, nextInternalID))

    node
  }
  
  private def createNode(video: Video, nextInternalID: Int): Node = {
    val node = graphDatabaseService.createNode(Indexes.VideoLink.label)

    node.setProperty(FieldNames.VideoLink, video.uri.toString)
    node.setProperty(FieldNames.VideoOrigin, video.origin.toString)
    node.setProperty(FieldNames.InternalVideoID, nextInternalID)

    node
  }
}
