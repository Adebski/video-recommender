package ztis.twitter

import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.{UserAndRating, CassandraClient, UserOrigin}

import scalaj.http.HttpOptions

class ProcessTweetTask(cassandraClient: CassandraClient, tweet: Tweet) extends Runnable with StrictLogging {

  override def run(): Unit = {
    val links = TweetURLExtractor.extractLinks(tweet)

    val resolvedLinks = links.flatMap(followLink).map(java.net.URI.create(_))
    val videoLinks = resolvedLinks.filter(link => ProcessTweetTask.AcceptedDomains.contains(link.getHost))
    if (videoLinks.nonEmpty) {
      logger.info("Extracted video links " + videoLinks + " from " + resolvedLinks)
      resolvedLinks.foreach(persist)
    }
  }

  private def followLink(link: String): Option[String] = {
    scalaj.http.Http(link)
      .option(HttpOptions.followRedirects(true))
      .method("GET")
      .asBytes.location
  }

  private def persist(uri: java.net.URI): Unit = {
    val uriString = uri.toString
    val userAndRating = UserAndRating(tweet.userName(), 
      UserOrigin.Twitter, 
      uriString, 
      rating = 1, 
      timesUpvotedByFriends = 0)
    
    cassandraClient.updateRating(userAndRating)
  }
}

object ProcessTweetTask {
  val AcceptedDomains = Vector("youtube.com", "vimeo.com", "www.youtube.com", "www.vimeo.com")
}
