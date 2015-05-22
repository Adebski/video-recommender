package ztis.twitter

import java.io.IOException

import akka.actor._
import akka.event.LoggingReceive
import com.twitter.Extractor
import ztis.cassandra.CassandraClient
import ztis.relationships.RelationshipFetcherProducer
import ztis.twitter.TweetProcessorActor.{ProcessTweet, Timeout}
import ztis.user_video_service.UserServiceActor.{RegisterTwitterUser, TwitterUserRegistered}
import ztis.user_video_service.VideoServiceActor.{RegisterVideos, Video, VideosRegistered}
import ztis.{UserOrigin, UserVideoRating, VideoOrigin}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scalaj.http.HttpOptions

object TweetProcessorActor {

  private case class ProcessTweet(tweet: Tweet)

  private case object Timeout

  private val extractor = new Extractor

  private def extractLinks(tweet: Tweet): Vector[String] = {
    val links = extractor.extractURLs(tweet.text())
    links.asScala.toVector
  }

  def props(tweet: Tweet,
            timeout: FiniteDuration,
            cassandraClient: CassandraClient,
            userServiceActor: ActorRef,
            videoServiceActor: ActorRef,
            relationshipsFetcher: RelationshipFetcherProducer): Props = {
    Props(classOf[TweetProcessorActor], tweet, timeout, cassandraClient, userServiceActor, videoServiceActor, relationshipsFetcher)
  }
}

class TweetProcessorActor(tweet: Tweet,
                          timeout: FiniteDuration,
                          cassandraClient: CassandraClient,
                          userServiceActor: ActorRef,
                          videoServiceActor: ActorRef,
                          relationshipsFetcher: RelationshipFetcherProducer) extends Actor with ActorLogging {

  self ! TweetProcessorActor.ProcessTweet(tweet)

  private var userResponse: Option[TwitterUserRegistered] = None

  private var videoResponse: Option[VideosRegistered] = None

  private var timeoutMessage: Option[Cancellable] = None

  override def receive: Receive = LoggingReceive {
    case ProcessTweet(tweet) => {
      val links = TweetProcessorActor.extractLinks(tweet)
      val resolvedLinks = links.flatMap(followLink).map(java.net.URI.create(_))
      val videos = resolvedLinks.flatMap { link =>
        val origin: Option[VideoOrigin] = VideoOrigin.recognize(link.getHost)
        origin.map(Video(_, link))
      }

      if (videos.nonEmpty) {
        log.info(s"Extracted videos $videos from $resolvedLinks that were resolved from $links")
        userServiceActor ! RegisterTwitterUser(tweet.userName(), tweet.userId())
        videoServiceActor ! RegisterVideos(videos)
        scheduleTimeout()
      } else {
        context.stop(self)
      }
    }
    case response: TwitterUserRegistered => {
      userResponse = Some(response)

      if (bothResponsesReceived) {
        processResponses()
      }
    }
    case response: VideosRegistered => {
      videoResponse = Some(response)

      if (bothResponsesReceived) {
        processResponses()
      }
    }
  }

  private def bothResponsesReceived: Boolean = {
    userResponse.nonEmpty && videoResponse.nonEmpty
  }

  private def processResponses(): Unit = {
    try {
      val userID = userResponse.get.internalUserID
      val videoIDWithOrigin: Vector[(Int, Video)] =
        videoResponse.get.internalVideoIDs.zip(videoResponse.get.request.videos)

      videoIDWithOrigin.foreach { videoInformation =>
        val videoID = videoInformation._1
        val videoOrigin = videoInformation._2.origin
        val toPersist =
          UserVideoRating(userID, UserOrigin.Twitter, videoID, videoOrigin, 1)

        log.info(s"Persisting $toPersist")
        cassandraClient.updateRating(toPersist, userResponse.get.followedBy)

        relationshipsFetcher.requestRelationshipsForTwitterUser(tweet.userId())
      }
    } catch {
      case e: Exception => {
        throw new IllegalArgumentException(s"Could not persist $userResponse and $videoResponse", e)
      }
    } finally {
      timeoutMessage.foreach(_.cancel())
      context.stop(self)
    }
  }

  private def followLink(link: String): Option[String] = {
    try {
      scalaj.http.Http(link)
        .option(HttpOptions.followRedirects(true))
        .method("GET")
        .asBytes.location
    } catch {
      case e: IOException => {
        throw new IllegalArgumentException(s"Could not resolve link $link", e)
      }
    } finally {
      timeoutMessage.foreach(_.cancel())
    }
  }

  private def scheduleTimeout(): Unit = {
    import context.dispatcher
    timeoutMessage = Some(context.system.scheduler.scheduleOnce(timeout, self, Timeout))
  }
}
