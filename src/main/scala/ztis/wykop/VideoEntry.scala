package ztis.wykop

import ztis.user_video_service.VideoServiceActor.Video

case class VideoEntry(userName: String, video: Video)
