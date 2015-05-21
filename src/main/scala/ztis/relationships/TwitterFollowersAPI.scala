package ztis.relationships

import twitter4j.api.FriendsFollowersResources
import twitter4j.{PagableResponseList, User}
import ztis.twitter.TwitterUser

import scala.collection.mutable.ArrayBuffer

/**
 * Simple adapter for Twitter4J because of problematic PagableResponseList class - hard to test
 * @param twitterAPI
 */
class TwitterFollowersAPI(twitterAPI: FriendsFollowersResources) {
  private val initialCursor = -1L

  def followersFor(userID: Long): Vector[TwitterUser] = {
    val buffer = ArrayBuffer.empty[TwitterUser]
    var hasMoreUsers = true
    var cursor = initialCursor

    while (hasMoreUsers) {
      val response: PagableResponseList[User] = twitterAPI.getFollowersList(userID, cursor)
      var i = 0

      while (i < response.size()) {
        val user = response.get(i)
        buffer += TwitterUser(user.getId, user.getName)
        i += 1
      }

      if (response.hasNext) {
        cursor = response.getNextCursor
      } else {
        hasMoreUsers = false
      }
    }

    buffer.toVector
  }
}
