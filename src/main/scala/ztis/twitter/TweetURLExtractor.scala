package ztis.twitter

import com.twitter.Extractor

/**
 * Created by adebski on 19.04.15.
 */
object TweetURLExtractor {
  private val extractor = new Extractor
  
  def extractLinks(tweet: Tweet): Vector[String] = {
    val links = extractor.extractURLs(tweet.text())
    links.asScala.toVector
  }
}
