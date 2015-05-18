package ztis

case class UserVideoRating(userID: Int,
                         userOrigin: UserOrigin,
                         videoID: Int,
                         videoOrigin: VideoOrigin,
                         rating: Int) {
  def toTuple: (Int, String, Int, String, Int) = {
    (userID, userOrigin.toString, videoID, videoOrigin.toString, rating)
  }
}
