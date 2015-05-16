package ztis.cassandra

import com.datastax.driver.core.Row
import ztis.{UserAndRating, UserOrigin, VideoOrigin}

import scala.collection.JavaConverters._

class UserAndRatingRepository(client: CassandraClient) {

  def allRatings(): Vector[UserAndRating] = {
    val resultSet = client.allRatings
    val iterator = resultSet.iterator().asScala

    iterator.map(toUserAndRating).toVector
  }
        
  private def toUserAndRating(row: Row): UserAndRating = {
    val userId = row.getInt("user_id")
    val userOrigin = UserOrigin.fromString(row.getString("user_origin"))
    val videoID = row.getInt("video_id")
    val videoOrigin = VideoOrigin.fromString(row.getString("video_origin"))
    val rating = row.getInt("rating")
    val timesUpvotedByFriends = row.getInt("timesUpvotedByFriends")

    UserAndRating(userId, userOrigin, videoID, videoOrigin, rating, timesUpvotedByFriends)
  }

}
