package ztis

import com.twitter.Extractor

import scala.collection.JavaConverters._

object TweetURLExtractor {
  private val extractor = new Extractor
  
  def extractLinks(tweet: Tweet): Vector[String] = {
    val links = extractor.extractURLs(tweet.text())
    links.asScala.toVector
  }
}
