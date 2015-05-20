package ztis.user_video_service.persistence

import org.neo4j.graphdb.DynamicLabel

object Labels {
  val TwitterUser = DynamicLabel.label("TwitterUser")
  val WykopUser = DynamicLabel.label("WykopUser")
  val Video = DynamicLabel.label("Video")
  val Metadata = DynamicLabel.label("Metadata")
  val NextUserInternalID = DynamicLabel.label("NextUserInternalID")
  val NextVideoInternalID = DynamicLabel.label("NextVideoInternalID")
}
