package ztis.user_video_service

import java.net.URI

import ztis.VideoOrigin
import ztis.user_video_service.ServiceActorMessages.{NextInternalIDRequest, NextInternalIDResponse}
import ztis.user_video_service.VideoServiceActor.{RegisterVideo, VideoRegistered}
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

    "create new video and return the same internal id for subsequent requests" in {
      // given
      val actor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))
      val videoRequest = RegisterVideo(VideoOrigin.YouTube, URI.create("http://youtube.com"))

      // when
      actor ! videoRequest
      actor ! videoRequest

      // then
      val firstVideoResponse = expectMsgClass(classOf[VideoRegistered])
      val secondVideoResponse = expectMsgClass(classOf[VideoRegistered])

      assert(firstVideoResponse == secondVideoResponse)
      assert(firstVideoResponse.internalVideoID == 0)
    }

    "load nextInternalID from database" in {
      val actor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))

      // when
      actor ! RegisterVideo(VideoOrigin.YouTube, URI.create("http://youtube.com/watch?v=1"))
      actor ! RegisterVideo(VideoOrigin.YouTube, URI.create("http://youtube.com/watch?v=2"))
      actor ! RegisterVideo(VideoOrigin.YouTube, URI.create("http://youtube.com/watch?v=3"))
      expectMsgClass(classOf[VideoRegistered])
      expectMsgClass(classOf[VideoRegistered])
      expectMsgClass(classOf[VideoRegistered])

      system.stop(actor)

      // then
      val newActor = system.actorOf(VideoServiceActor.props(graphDb, videoRepository, metadataRepository))

      newActor ! NextInternalIDRequest
      val response = expectMsgClass(classOf[NextInternalIDResponse])
      assert(response.nextInternalID == 3)
    }
  }
}
