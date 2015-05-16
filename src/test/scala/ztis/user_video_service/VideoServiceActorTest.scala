package ztis.user_video_service

import java.net.URI

import ztis.VideoOrigin
import ztis.user_video_service.ServiceActorMessages.{NextInternalIDRequest, NextInternalIDResponse}
import ztis.user_video_service.VideoServiceActor.{Video, RegisterVideos, VideosRegistered}
import ztis.user_video_service.persistence.VideoRepository

class VideoServiceActorTest extends ServiceActorTest {
  val videoRepository = new VideoRepository(graphDb)

  "VideoServiceActor" should {
    "return initial nextInternalID value" in {
      // given
      val actor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
      actor ! NextInternalIDRequest

      // when
      val response = expectMsgClass(classOf[NextInternalIDResponse])
      assert(response.nextInternalID == 0)
    }

    "handle the situation when there are duplicated videos in single request" in {
      // given
      val actor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
      val videosToRegister = Vector(
        Video(VideoOrigin.YouTube, URI.create("http://youtube.com")),
        Video(VideoOrigin.YouTube, URI.create("http://youtube.com"))
      )
      val videoRequest = RegisterVideos(videosToRegister)

      // when
      actor ! videoRequest
      actor ! NextInternalIDRequest
      
      // then
      val videosRegistered = expectMsgClass(classOf[VideosRegistered])
      val nextInternalID = expectMsgClass(classOf[NextInternalIDResponse])
      
      assert(videosRegistered.internalVideoIDs == Vector(0, 0)) 
      assert(nextInternalID.nextInternalID == 1)
    }
    
    "create new videos and return the same internal id for subsequent requests" in {
      // given
      val actor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
      val videosToRegister = Vector(
        Video(VideoOrigin.YouTube, URI.create("http://youtube.com")),
        Video(VideoOrigin.Vimeo, URI.create("http://vimeo.com"))
      )
      val videoRequest = RegisterVideos(videosToRegister)

      // when
      actor ! videoRequest
      actor ! videoRequest

      // then
      val firstVideoResponse = expectMsgClass(classOf[VideosRegistered])
      val secondVideoResponse = expectMsgClass(classOf[VideosRegistered])

      assert(firstVideoResponse == secondVideoResponse)
      assert(firstVideoResponse.internalVideoIDs == Vector(0, 1))
    }

    "load nextInternalID from database" in {
      val actor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
      val firstVideo = Video(VideoOrigin.YouTube, URI.create("http://youtube.com/watch?v=1"))
      val secondVideo = Video(VideoOrigin.YouTube, URI.create("http://youtube.com/watch?v=2"))
      val thirdVideo = Video(VideoOrigin.YouTube, URI.create("http://youtube.com/watch?v=3"))
      
      // when
      actor ! RegisterVideos(Vector(firstVideo))
      actor ! RegisterVideos(Vector(secondVideo))
      actor ! RegisterVideos(Vector(thirdVideo))
      expectMsgClass(classOf[VideosRegistered])
      expectMsgClass(classOf[VideosRegistered])
      expectMsgClass(classOf[VideosRegistered])

      system.stop(actor)

      // then
      val newActor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))

      newActor ! NextInternalIDRequest
      val response = expectMsgClass(classOf[NextInternalIDResponse])
      assert(response.nextInternalID == 3)
    }
  }
}
