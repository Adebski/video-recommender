package ztis

case class UserAndRating(userName: String,
                         origin: UserOrigin,
                         link: String,
                         rating: Int,
                         timesUpvotedByFriends: Int) {
  def toTuple: (String, String, String, Int, Int) = {
    (userName, origin.name, link, rating, timesUpvotedByFriends)
  }
}