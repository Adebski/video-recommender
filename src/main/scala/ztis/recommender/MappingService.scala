package ztis.recommender

trait MappingService {
  def identifyUser(twitterName: String, wykopName: String): (Option[Int], Option[Int])

  def resolveVideo(videoId: Int): Option[Video]
}
