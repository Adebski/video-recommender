package ztis

case class UserAndRating(userID: Int,
                         userOrigin: UserOrigin,
                         videoID: Int,
                         videoOrigin: VideoOrigin,
                         rating: Int,
                         timesUpvotedByFriends: Int) {
  def toTuple: (Int, String, Int, String, Int, Int) = {
    (userID, userOrigin.toString, videoID, videoOrigin.toString, rating, timesUpvotedByFriends)
  }
}
