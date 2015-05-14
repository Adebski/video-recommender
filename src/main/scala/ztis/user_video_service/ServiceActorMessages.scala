package ztis.user_video_service

object ServiceActorMessages {

  case object NextInternalIDRequest

  case class NextInternalIDResponse(nextInternalID: Int)

}
