package ztis.cassandra

import com.datastax.driver.core.Row
import ztis.{UserVideoImplicitAssociation, UserVideoRating, UserOrigin, VideoOrigin}

import scala.collection.JavaConverters._

class UserVideoRatingRepository(client: CassandraClient) {

  def allRatings(): Vector[UserVideoRating] = {
    val resultSet = client.allRatings
    val iterator = resultSet.iterator().asScala

    iterator.map(toUserVideoRating).toVector
  }
        
  def allAssociations(): Vector[UserVideoImplicitAssociation] = {
    val resultSet = client.allAssociations
    val iterator = resultSet.iterator().asScala

    iterator.map(toAssociation).toVector
  }
  
  private def toUserVideoRating(row: Row): UserVideoRating = {
    val userId = row.getInt("user_id")
    val userOrigin = UserOrigin.fromString(row.getString("user_origin"))
    val videoID = row.getInt("video_id")
    val videoOrigin = VideoOrigin.fromString(row.getString("video_origin"))
    val rating = row.getInt("rating")

    UserVideoRating(userId, userOrigin, videoID, videoOrigin, rating)
  }

  private def toAssociation(row: Row): UserVideoImplicitAssociation = {
    val userId = row.getInt("user_id")
    val userOrigin = UserOrigin.fromString(row.getString("user_origin"))
    val followedUserId = row.getInt("followed_user_id")
    val followedUserOrigin = UserOrigin.fromString(row.getString("followed_user_origin"))
    val videoID = row.getInt("video_id")
    val videoOrigin = VideoOrigin.fromString(row.getString("video_origin"))

    UserVideoImplicitAssociation(userId, userOrigin, followedUserId, followedUserOrigin, videoID, videoOrigin)
  }
  
}
