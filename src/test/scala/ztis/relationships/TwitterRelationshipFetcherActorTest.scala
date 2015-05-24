package ztis.relationships

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.mockito.Mockito._
import org.scalatest.FlatSpecLike
import org.scalatest.mock.MockitoSugar
import twitter4j.{RateLimitStatus, TwitterException}
import ztis.cassandra.SparkCassandraClient
import ztis.relationships.TwitterRelationshipFetcherActor.FetchRelationshipsTwitter
import ztis.twitter.TwitterUser
import ztis.user_video_service.UserServiceActor.CreateRelationshipsToTwitterUser
import scala.concurrent.duration._

class TwitterRelationshipFetcherActorTest extends TestKit(ActorSystem("test-actor-system", ConfigFactory.load("akka"))) with FlatSpecLike with MockitoSugar with ImplicitSender {

  "TwitterRelationshipFetcherActor" should "handle limits in Twitter API" in {
    // given
    val waitSeconds = 10
    val api = mock[TwitterFollowersAPI]
    val rateLimitStatus = mock[RateLimitStatus]
    val twitterException = mock[TwitterException]
    val sparkCassandraClient = mock[SparkCassandraClient]
    val userServiceActor = TestProbe()
    val fetcher = system.actorOf(TwitterRelationshipFetcherActor.props(api, userServiceActor.ref, sparkCassandraClient))
    val partialBuilder = FollowersBuilder(15, Vector(TwitterUser(20, "first-user"), TwitterUser(30, "second-user")), partialResult = true)
    val finalBuilderTwitterUsers = partialBuilder.gatheredFollowers :+ TwitterUser(40, "last-user")
    val finalBuilder =
      FollowersBuilder(TwitterFollowersAPI.FinalCursorValue, finalBuilderTwitterUsers, partialResult = false)
    val secondUserFinalBuilderTwitterUsers = Vector(TwitterUser(100, "second-user-follower1"), TwitterUser(200, "second-user-follower2"))
    val secondUserFinalBuilder =
      FollowersBuilder(TwitterFollowersAPI.FinalCursorValue, secondUserFinalBuilderTwitterUsers, partialResult = false)
    val rateExceededException = FollowersFetchingLimitException(partialBuilder, twitterException)

    when(rateLimitStatus.getSecondsUntilReset).thenReturn(waitSeconds)
    when(twitterException.getRateLimitStatus).thenReturn(rateLimitStatus)
    when(api.followersFor(1L, FollowersBuilder(TwitterFollowersAPI.InitialCursor))).thenThrow(rateExceededException)
    when(api.followersFor(1L, partialBuilder)).thenReturn(finalBuilder)
    when(api.followersFor(2L, FollowersBuilder(TwitterFollowersAPI.InitialCursor))).thenReturn(secondUserFinalBuilder)

    // when
    fetcher ! FetchRelationshipsTwitter(1L)
    fetcher ! FetchRelationshipsTwitter(2L)

    // then
    Thread.sleep(waitSeconds.seconds.toMillis)
    
    userServiceActor.expectMsg(CreateRelationshipsToTwitterUser(1L, finalBuilderTwitterUsers))
    userServiceActor.expectMsg(CreateRelationshipsToTwitterUser(2L, secondUserFinalBuilderTwitterUsers))
  }
}
