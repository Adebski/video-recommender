package ztis.twitter

import java.net.URL
import java.util.Date

import twitter4j.{RateLimitStatus, Status, URLEntity, User}

class TestUser(name: String, id: Long) extends User {
  override def getBiggerProfileImageURL: String = ???

  override def isProtected: Boolean = ???

  override def isTranslator: Boolean = ???

  override def getProfileLinkColor: String = ???

  override def getProfileImageURL: String = ???

  override def getProfileBackgroundImageUrl: String = ???

  override def getProfileBannerIPadRetinaURL: String = ???

  override def getMiniProfileImageURLHttps: String = ???

  override def getProfileSidebarFillColor: String = ???

  override def getScreenName: String = ???

  override def getListedCount: Int = ???

  override def getOriginalProfileImageURLHttps: String = ???

  override def isProfileBackgroundTiled: Boolean = ???

  override def isProfileUseBackgroundImage: Boolean = ???

  override def getUtcOffset: Int = ???

  override def getProfileSidebarBorderColor: String = ???

  override def isContributorsEnabled: Boolean = ???

  override def getTimeZone: String = ???

  override def getName: String = name

  override def getCreatedAt: Date = ???

  override def getDescriptionURLEntities: Array[URLEntity] = ???

  override def getURL: String = ???

  override def getLang: String = ???

  override def getId: Long = id

  override def getProfileImageURLHttps: String = ???

  override def getStatus: Status = ???

  override def getMiniProfileImageURL: String = ???

  override def getDescription: String = ???

  override def getProfileBannerRetinaURL: String = ???

  override def getFollowersCount: Int = ???

  override def isGeoEnabled: Boolean = ???

  override def getURLEntity: URLEntity = ???

  override def getProfileBackgroundColor: String = ???

  override def isFollowRequestSent: Boolean = ???

  override def getProfileBannerMobileURL: String = ???

  override def getFavouritesCount: Int = ???

  override def getProfileBannerURL: String = ???

  override def getProfileBackgroundImageUrlHttps: String = ???

  override def getProfileBackgroundImageURL: String = ???

  override def isVerified: Boolean = ???

  override def getProfileImageUrlHttps: URL = ???

  override def getLocation: String = ???

  override def getFriendsCount: Int = ???

  override def getProfileBannerMobileRetinaURL: String = ???

  override def getProfileTextColor: String = ???

  override def getStatusesCount: Int = ???

  override def isShowAllInlineMedia: Boolean = ???

  override def getProfileBannerIPadURL: String = ???

  override def getOriginalProfileImageURL: String = ???

  override def getBiggerProfileImageURLHttps: String = ???

  override def getAccessLevel: Int = ???

  override def getRateLimitStatus: RateLimitStatus = ???

  override def compareTo(o: User): Int = ???
}
