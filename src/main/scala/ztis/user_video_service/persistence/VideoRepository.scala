package ztis.user_video_service.persistence

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import ztis.user_video_service.FieldNames
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
