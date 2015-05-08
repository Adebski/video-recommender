package ztis

import com.datastax.driver.core.Row
import ztis.cassandra.CassandraClient

import scala.collection.JavaConverters._

class UserAndRatingRepository(client: CassandraClient) {

  def allRatings(): Vector[UserAndRating] = {
    val resultSet = client.allRatings()
    val iterator = resultSet.iterator().asScala

    iterator.map(toUserAndRating).toVector
  }

  private def toUserAndRating(row: Row): UserAndRating = {
    val userName = row.getString("user_id")
    val origin = UserOrigin.fromString(row.getString("user_origin"))
    val link = row.getString("link")
    val rating = row.getInt("rating")
    val timesUpvotedByFriends = row.getInt("timesUpvotedByFriends")

    UserAndRating(userName, origin, link, rating, timesUpvotedByFriends)
  }

}
