package ztis

import com.typesafe.scalalogging.slf4j.StrictLogging

import scalaj.http.HttpOptions

class ProcessTweetTask(tweet: Tweet) extends Runnable with StrictLogging {
  
  override def run(): Unit = {
    val links = TweetURLExtractor.extractLinks(tweet)

    val resolvedLinks =   links.flatMap(followLink).map(java.net.URI.create(_))
    val videoLinks = resolvedLinks.filter(link => ProcessTweetTask.AcceptedDomains.contains(link.getHost))
    if (videoLinks.nonEmpty) {
      logger.info("Extracted video links " + videoLinks + " from " + resolvedLinks)
    }
  }

  private def followLink(link: String): Option[String] = {
    scalaj.http.Http(link)
      .option(HttpOptions.followRedirects(true))
      .method("GET")
      .asBytes.location
  }
}

object ProcessTweetTask {
  val AcceptedDomains = Vector("youtube.com", "vimeo.com", "www.youtube.com", "www.vimeo.com")
}
