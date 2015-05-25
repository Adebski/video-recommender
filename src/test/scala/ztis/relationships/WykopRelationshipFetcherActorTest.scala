package ztis.relationships

import akka.testkit.TestProbe
import org.mockito.Mockito._
import ztis.AkkaSpec
import ztis.cassandra.SparkCassandraClient
import ztis.relationships.WykopRelationshipFetcherActor.FetchRelationshipsWykop
import ztis.user_video_service.UserServiceActor.CreateRelationshipsToWykopUser
import ztis.wykop._

import scala.concurrent.duration._

class WykopRelationshipFetcherActorTest extends AkkaSpec {

  "WykopRelationshipFetcherActor" should "handle limits in Wykop API" in {
    // given
    val waitFor = 10.seconds
    val api = mock[WykopAPI]
    val sparkCassandraClient = mock[SparkCassandraClient]
    val userServiceActor = TestProbe()
    val fetcher = system.actorOf(WykopRelationshipFetcherActor.props(api, userServiceActor.ref, sparkCassandraClient))
    val partialBuilder = FollowersBuilder(15, Vector("first-user", "second-user"), partialResult = true)
    val finalBuilderWykopUsers = partialBuilder.gatheredFollowers :+ "last-user"
    val finalBuilder =
      FollowersBuilder(WykopAPI.FinalPage, finalBuilderWykopUsers, partialResult = false)
    val secondUserFinalBuilderWykopUsers = Vector("second-user-follower1", "second-user-follower2")
    val secondUserFinalBuilder =
      FollowersBuilder(WykopAPI.FinalPage, secondUserFinalBuilderWykopUsers, partialResult = false)
    val wykopError = WykopAPIError(code = WykopAPI.RateExceededErrorCode, "Rate exceeded")
    val wykopException = WykopAPIException(wykopError)
    val rateExceededException = WykopFollowersFetchingLimitException(partialBuilder, wykopException, waitFor = waitFor)

    when(api.usersFollowingUser("user1", FollowersBuilder(WykopAPI.InitialPage))).thenThrow(rateExceededException)
    when(api.usersFollowingUser("user1", partialBuilder)).thenReturn(finalBuilder)
    when(api.usersFollowingUser("user2", FollowersBuilder(WykopAPI.InitialPage))).thenReturn(secondUserFinalBuilder)

    // when
    fetcher ! FetchRelationshipsWykop("user1")
    fetcher ! FetchRelationshipsWykop("user2")

    // then
    Thread.sleep(waitFor.toMillis)

    userServiceActor.expectMsg(CreateRelationshipsToWykopUser("user1", finalBuilderWykopUsers))
    userServiceActor.expectMsg(CreateRelationshipsToWykopUser("user2", secondUserFinalBuilderWykopUsers))
  }
}
