package ztis.wykop

import ztis.relationships.FollowersBuilder

import scala.concurrent.duration._

case class WykopFollowersFetchingLimitException(partialBuilder: FollowersBuilder[String], 
                                                reason: WykopAPIException, 
                                                waitFor: FiniteDuration = WykopAPI.DefaultWaitTime) 
  extends RuntimeException(s"Wykop API rate limit exceeded when fetching followers", reason)
