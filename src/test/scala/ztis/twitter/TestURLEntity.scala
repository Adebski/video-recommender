package ztis.twitter

import twitter4j.URLEntity

class TestURLEntity(expandedURL: String) extends URLEntity {
  override def getDisplayURL: String = ???

  override def getURL: String = ???

  override def getStart: Int = ???

  override def getEnd: Int = ???

  override def getExpandedURL: String = expandedURL
}
