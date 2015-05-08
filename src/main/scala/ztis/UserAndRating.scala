package ztis

case class UserAndRating(userName: String,
                         origin: UserOrigin,
                         link: String,
                         rating: Int,
                         timesUpvotedByFriends: Int)