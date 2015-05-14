package ztis.user_video_service

import ztis.user_video_service.ServiceActorMessages.{NextInternalIDRequest, NextInternalIDResponse}
import ztis.user_video_service.UserServiceActor._
import ztis.user_video_service.persistence._

class UserServiceActorTest extends ServiceActorTest {
  val userRepository = new UserRepository(graphDb)

  "UserServiceActor" should {
    "return initial nextInternalID value" in {
      // given
      val actor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))
      actor ! NextInternalIDRequest

      // when
      val response = expectMsgClass(classOf[NextInternalIDResponse])
      assert(response.nextInternalID == 0)
    }

    "create new user and return the same internal id for subsequent requests" in {
      // given
      val actor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))
      val twitterRequest = RegisterTwitterUser("username", 123)
      val wykopRequest = RegisterWykopUser("username")

      // when
      actor ! twitterRequest
      actor ! wykopRequest
      actor ! twitterRequest
      actor ! wykopRequest

      // then
      val firstTwitterResponse = expectMsgClass(classOf[TwitterUserRegistered])
      val firstWykopResponse = expectMsgClass(classOf[WykopUserRegistered])
      val secondTwitterResponse = expectMsgClass(classOf[TwitterUserRegistered])
      val secondWykopResponse = expectMsgClass(classOf[WykopUserRegistered])
      assert(firstTwitterResponse == secondTwitterResponse)
      assert(firstWykopResponse == secondWykopResponse)
      assert(firstTwitterResponse.internalUserID == 0)
      assert(firstWykopResponse.internalUserID == 1)
    }

    "load nextInternalID from database" in {
      val actor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))

      // when
      actor ! RegisterTwitterUser("username1", 123)
      actor ! RegisterTwitterUser("username2", 124)
      actor ! RegisterTwitterUser("username3", 125)
      expectMsgClass(classOf[TwitterUserRegistered])
      expectMsgClass(classOf[TwitterUserRegistered])
      expectMsgClass(classOf[TwitterUserRegistered])

      system.stop(actor)

      // then
      val newActor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))

      newActor ! NextInternalIDRequest
      val response = expectMsgClass(classOf[NextInternalIDResponse])
      assert(response.nextInternalID == 3)
    }
  }
}
