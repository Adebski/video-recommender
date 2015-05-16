package ztis.user_video_service

import java.net.URI

import ztis.VideoOrigin
import ztis.user_video_service.UserServiceActor.{RegisterTwitterUser, RegisterWykopUser, TwitterUserRegistered, WykopUserRegistered}
import ztis.user_video_service.UserVideoServiceQueryActor.{VideoExternalInformationResponse, VideoExternalInformationRequest, UserInternalIDRequest, UserInternalIDResponse}
import ztis.user_video_service.VideoServiceActor.{VideosRegistered, Video, RegisterVideos}
import ztis.user_video_service.persistence.{UserRepository, VideoRepository}

class UserVideoServiceQueryActorTest extends ServiceActorTest {
  val userRepository = new UserRepository(graphDb)
  val videoRepository = new VideoRepository(graphDb)

  "UserVideoServiceQueryActor" should {
    "reply with information about internal IDs" in {
      // given
      val userServiceActor = system.actorOf(UserServiceActor.props(graphDb, userRepository, metadataRepository), "user-service-actor")
      val videoServiceActor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository), "video-service-actor")
      val queryActor = system.actorOf(UserVideoServiceQueryActor.props(graphDb, userRepository, videoRepository), "user-video-service-query-actor")
      val video = Video(VideoOrigin.YouTube, URI.create("www.youtube.com"))
      
      userServiceActor ! RegisterTwitterUser("twitter-user-name", 1)
      userServiceActor ! RegisterWykopUser("wykop-user-name")
      val internalTwitterID = expectMsgClass(classOf[TwitterUserRegistered]).internalUserID
      val internalWykopID = expectMsgClass(classOf[WykopUserRegistered]).internalUserID

      videoServiceActor ! RegisterVideos(Vector(video))
      val internalVideoID = expectMsgClass(classOf[VideosRegistered]).internalVideoIDs(0)
      
      // when
      queryActor ! UserInternalIDRequest(None, None)
      queryActor ! UserInternalIDRequest(twitterUserName = Some("twitter-user-name"), wykopUserName = Some("twitter-user-name"))
      queryActor ! UserInternalIDRequest(twitterUserName = Some("wykop-user-name"), wykopUserName = Some("wykop-user-name"))
      queryActor ! UserInternalIDRequest(twitterUserName = Some("twitter-user-name"), wykopUserName = Some("wykop-user-name"))
      queryActor ! VideoExternalInformationRequest(internalVideoID)
      queryActor ! VideoExternalInformationRequest(internalVideoID + 1)
      
      // then
      expectMsg(UserInternalIDResponse(None, None))
      expectMsg(UserInternalIDResponse(internalTwitterUserID = Some(internalTwitterID), internalWykopUserID = None))
      expectMsg(UserInternalIDResponse(internalTwitterUserID = None, internalWykopUserID = Some(internalWykopID)))
      expectMsg(UserInternalIDResponse(internalTwitterUserID = Some(internalTwitterID), internalWykopUserID = Some(internalWykopID)))
      expectMsg(VideoExternalInformationResponse(Some(video)))
      expectMsg(VideoExternalInformationResponse(None))
    }
  }
}
