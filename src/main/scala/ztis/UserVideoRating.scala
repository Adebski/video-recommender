package ztis

/**
 * Represents information about user, video association when user explicitly rated some video.
 * @param userID
 * @param userOrigin
 * @param videoID
 * @param videoOrigin
 * @param rating
 */
case class UserVideoRating(userID: Int,
                           userOrigin: UserOrigin,
                           videoID: Int,
                           videoOrigin: VideoOrigin,
                           rating: Int) {
  def toTuple: (Int, String, Int, String, Int) = {
    (userID, userOrigin.toString, videoID, videoOrigin.toString, rating)
  }
}
