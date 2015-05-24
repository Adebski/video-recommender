package ztis.relationships

import com.typesafe.scalalogging.slf4j.StrictLogging
import twitter4j.api.FriendsFollowersResources
import twitter4j.{TwitterException, PagableResponseList, User}
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
class TwitterFollowersAPI(twitterAPI: FriendsFollowersResources) extends StrictLogging {

  def followersFor(userID: Long, builder: FollowersBuilder[TwitterUser]): FollowersBuilder[TwitterUser] = {
    var hasMoreUsers = true
    var page = builder.page
    var buffer = ArrayBuffer.empty[TwitterUser]
    
    try {
      while (hasMoreUsers) {
        val response: PagableResponseList[User] = twitterAPI.getFollowersList(userID, page)
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
          logger.warn(s"Exceeded rate limit for user $userID, message = ${e.getErrorMessage}, must retry in ${e.getRateLimitStatus.getSecondsUntilReset} seconds")
          
          throw FollowersFetchingLimitException(FollowersBuilder(page, builder.gatheredFollowers ++ buffer, partialResult = true), e)
        } else {
          throw e
        }
      }
    }
    
    FollowersBuilder(page, builder.gatheredFollowers ++ buffer, partialResult = false)
  }
}
