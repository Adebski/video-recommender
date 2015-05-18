package ztis.cassandra

import com.datastax.driver.core.Row
import ztis.{UserVideoRating, UserOrigin, VideoOrigin}

import scala.collection.JavaConverters._

class UserVideoRatingRepository(client: CassandraClient) {

  def allRatings(): Vector[UserVideoRating] = {
    val resultSet = client.allRatings
    val iterator = resultSet.iterator().asScala

    iterator.map(toUserVideoRating).toVector
  }
        
  private def toUserVideoRating(row: Row): UserVideoRating = {
    val userId = row.getInt("user_id")
    val userOrigin = UserOrigin.fromString(row.getString("user_origin"))
    val videoID = row.getInt("video_id")
    val videoOrigin = VideoOrigin.fromString(row.getString("video_origin"))
    val rating = row.getInt("rating")

    UserVideoRating(userId, userOrigin, videoID, videoOrigin, rating)
  }

}
