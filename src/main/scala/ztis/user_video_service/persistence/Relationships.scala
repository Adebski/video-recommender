package ztis.user_video_service.persistence

import org.neo4j.graphdb.DynamicRelationshipType

object Relationships {
  val FromFollowerToFollowedUser = DynamicRelationshipType.withName("follows")
}
