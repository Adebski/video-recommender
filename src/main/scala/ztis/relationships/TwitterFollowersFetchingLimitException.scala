package ztis.relationships

import twitter4j.TwitterException
import ztis.twitter.TwitterUser

case class TwitterFollowersFetchingLimitException(partialBuilder: FollowersBuilder[TwitterUser], reason: TwitterException) 
  extends RuntimeException(s"Twitter API rate limit exceeded, ${reason.getRateLimitStatus}", reason)
