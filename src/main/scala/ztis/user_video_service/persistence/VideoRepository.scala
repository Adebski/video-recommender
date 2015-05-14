package ztis.user_video_service.persistence

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import ztis.user_video_service.FieldNames
import ztis.user_video_service.VideoServiceActor.{RegisterVideo, VideoRegistered}

class VideoRepository(graphDatabaseService: GraphDatabaseService) extends StrictLogging {
  implicit val _service = graphDatabaseService

  def getOrCreateVideo(request: RegisterVideo, internalID: Int): VideoRegistered = {
    val node = Option(graphDatabaseService.findNode(Indexes.VideoLink.label, Indexes.VideoLink.property, request.uri.toString))
      .getOrElse(createNode(request, internalID))
    val id = node.getProperty(FieldNames.InternalVideoID).asInstanceOf[Int]

    VideoRegistered(id, request)
  }

  private def createNode(request: RegisterVideo, internalID: Int): Node = {
    val node = graphDatabaseService.createNode(Indexes.VideoLink.label)

    node.setProperty(FieldNames.VideoLink, request.uri.toString)
    node.setProperty(FieldNames.VideoOrigin, request.origin.toString)
    node.setProperty(FieldNames.InternalVideoID, internalID)

    node
  }
}
