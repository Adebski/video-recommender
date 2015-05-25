package ztis.relationships

import com.typesafe.scalalogging.slf4j.StrictLogging
import twitter4j.{PagableResponseList, TwitterException, User, ZTISTwitter}
import ztis.twitter.TwitterUser

import scala.collection.mutable.ArrayBuffer

object TwitterFollowersAPI {
  val InitialCursor: Long = -1L

  val FinalCursorValue: Long = 0L
}

/**
 * Simple adapter for Twitter4J because of problematic PagableResponseList class - hard to test
 * @param twitterAPI
 */
class TwitterFollowersAPI(twitterAPI: ZTISTwitter) extends StrictLogging {

  def followersFor(userID: Long, builder: FollowersBuilder[TwitterUser]): FollowersBuilder[TwitterUser] = {
    var hasMoreUsers = true
    var page = builder.page
    var buffer = ArrayBuffer.empty[TwitterUser]

    try {
      while (hasMoreUsers) {
        val response: PagableResponseList[User] = twitterAPI.getFollowersListMaxCount(userID, page)
        var i = 0

        while (i < response.size()) {
          val user = response.get(i)
          buffer += TwitterUser(user.getId, user.getName)
          i += 1
        }

        if (response.hasNext) {
          page = response.getNextCursor
        } else {
          hasMoreUsers = false
        }
      }
    } catch {
      case e: TwitterException => {
        if (e.exceededRateLimitation()) {
          val partialResult = FollowersBuilder(page, builder.gatheredFollowers ++ buffer, partialResult = true)
          logger.warn(s"Exceeded rate limit for user $userID, for now gathered ${partialResult.gatheredFollowers.size}, message = ${e.getErrorMessage}, must retry in ${e.getRateLimitStatus.getSecondsUntilReset} seconds")

          throw FollowersFetchingLimitException(partialResult, e)
        } else {
          throw e
        }
      }
    }

    FollowersBuilder(page, builder.gatheredFollowers ++ buffer, partialResult = false)
  }
}
