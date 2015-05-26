package ztis.twitter

import java.util.Date

import twitter4j._

class TestStatus(user: TestUser, urlEntites: Array[URLEntity], val isRetweet: Boolean) extends Status {
  override def getPlace: Place = ???

  override def isFavorited: Boolean = ???

  override def getCreatedAt: Date = ???

  override def getUser: User = user

  override def getContributors: Array[Long] = ???

  override def getRetweetedStatus: Status = ???

  override def getInReplyToScreenName: String = ???

  override def isTruncated: Boolean = ???

  override def getId: Long = ???

  override def getCurrentUserRetweetId: Long = ???

  override def isPossiblySensitive: Boolean = ???

  override def getRetweetCount: Long = ???

  override def getGeoLocation: GeoLocation = ???

  override def getInReplyToUserId: Long = ???

  override def getSource: String = ???

  override def getText: String = ???

  override def getInReplyToStatusId: Long = ???

  override def isRetweetedByMe: Boolean = ???

  override def getAccessLevel: Int = ???

  override def getRateLimitStatus: RateLimitStatus = ???

  override def getHashtagEntities: Array[HashtagEntity] = ???

  override def getURLEntities: Array[URLEntity] = urlEntites

  override def getMediaEntities: Array[MediaEntity] = ???

  override def getUserMentionEntities: Array[UserMentionEntity] = ???

  override def compareTo(o: Status): Int = ???
}
