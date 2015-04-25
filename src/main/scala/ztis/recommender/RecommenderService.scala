package ztis.recommender

import scala.util.Random

class RecommenderService {
  val random = new Random()

  def recommend(request: RecommendRequest) : Either[List[Video], NoUserData.type] = {
    if(random.nextBoolean()) {
      Right(NoUserData)
    }
    else {
      Left(List(
        Video("youtube.com/watch?v=EWSRbRUH7pg"),
        Video("youtube.com/watch?v=MEgyGamo79I"),
        Video("youtube.com/watch?v=sPiWg5jSoZI"),
        Video("youtube.com/watch?v=0jEmxRJoAtQ")
      ))
    }
  }
}
