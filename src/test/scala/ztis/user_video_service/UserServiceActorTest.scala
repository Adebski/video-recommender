package ztis.user_video_service

import org.neo4j.graphdb.Direction
import ztis.twitter.TwitterUser
import ztis.user_video_service.ServiceActorMessages.{NextInternalIDRequest, NextInternalIDResponse}
import ztis.user_video_service.UserServiceActor._
import ztis.user_video_service.persistence._

import scala.collection.JavaConverters._

class UserServiceActorTest extends ServiceActorTest with UnitOfWork {
  val userRepository = new UserRepository(graphDb)

  val firstTwitterUser = TwitterUser(123, "username1")
  val secondTwitterUser = TwitterUser(124, "username2")
  val thirdTwitterUser = TwitterUser(125, "username3")
  val fourthTwitterUser = TwitterUser(126, "username4")
  
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
      assert(firstTwitterResponse.followedBy.isEmpty)
      
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

    "create relationships between users and do not duplicate relationships between the same pair of users" in {
      // given
      val actor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))

      actor ! RegisterTwitterUser(firstTwitterUser.userName, firstTwitterUser.userID)
      actor ! RegisterTwitterUser(secondTwitterUser.userName, secondTwitterUser.userID)
      actor ! RegisterTwitterUser(fourthTwitterUser.userName, fourthTwitterUser.userID)

      val firstInternalUserID = expectMsgClass(classOf[TwitterUserRegistered]).internalUserID
      val secondInternalUserID = expectMsgClass(classOf[TwitterUserRegistered]).internalUserID
      val fourthInternalUserID = expectMsgClass(classOf[TwitterUserRegistered]).internalUserID

      // when
      actor ! CreateRelationshipsToTwitterUser(firstTwitterUser.userID, Vector(secondTwitterUser, thirdTwitterUser))
      actor ! CreateRelationshipsToTwitterUser(firstTwitterUser.userID, Vector(secondTwitterUser, thirdTwitterUser))
      actor ! CreateRelationshipsToTwitterUser(fourthTwitterUser.userID, Vector(secondTwitterUser))
      val firstResponse = expectMsgClass(classOf[ToTwitterUserRelationshipsCreated])
      val secondResponse = expectMsgClass(classOf[ToTwitterUserRelationshipsCreated])
      val thirdResponse = expectMsgClass(classOf[ToTwitterUserRelationshipsCreated])

      // then
      assert(firstResponse == secondResponse)
      assert(firstResponse.internalUserID == firstInternalUserID)
      assert(firstResponse.fromUsers.toSet == Set(secondInternalUserID, fourthInternalUserID + 1))
      assert(thirdResponse.internalUserID == fourthInternalUserID)
      assert(thirdResponse.fromUsers.toSet == Set(secondInternalUserID))
      implicit val _db = graphDb
      unitOfWork { () =>
        val index = Indexes.TwitterUserInternalUserID
        val internalID: java.lang.Integer = secondInternalUserID
        val node = graphDb.findNode(index.label, index.property, internalID)
        val relationships = node.getRelationships(Direction.OUTGOING).asScala.toVector

        assert(relationships.size == 2)
      }
    }
    
    "return information about existing followers when registering already present Twitter user " in {
      // given
      val actor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository))

      // when
      actor ! RegisterTwitterUser(firstTwitterUser.userName, firstTwitterUser.userID)
      actor ! CreateRelationshipsToTwitterUser(firstTwitterUser.userID, Vector(secondTwitterUser, thirdTwitterUser))
      actor ! RegisterTwitterUser(firstTwitterUser.userName, firstTwitterUser.userID)
      
      // then
      val firstUserRegistered = expectMsgClass(classOf[TwitterUserRegistered])
      val relationshipsCreated = expectMsgClass(classOf[ToTwitterUserRelationshipsCreated])
      val firstUserRegisteredAgain = expectMsgClass(classOf[TwitterUserRegistered])
      
      assert(firstUserRegistered.internalUserID == firstUserRegisteredAgain.internalUserID)
      assert(relationshipsCreated.fromUsers.toSet == Set(firstUserRegistered.internalUserID + 1, firstUserRegistered.internalUserID + 2))
      assert(firstUserRegisteredAgain.followedBy.toSet == relationshipsCreated.fromUsers.toSet)
    }
  }
}
