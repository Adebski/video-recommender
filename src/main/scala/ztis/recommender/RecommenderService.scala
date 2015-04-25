package ztis.recommender

import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.util.Random

class RecommenderService extends StrictLogging {
  val random = new Random()

  def scheduleUserDataRetrieval(request: RecommendRequest) = {
    // TODO: put to the queue

    logger.info(s"Data not found. Scheduled twitter user ${request.twitterId} for high priority data retrieval")
    logger.info(s"Data not found. Scheduled wykop user ${request.wykopId} for high priority data retrieval")
  }

  def recommend(request: RecommendRequest) : Either[List[Video], NoUserData.type] = {
    if(random.nextBoolean()) {
      scheduleUserDataRetrieval(request)
      Right(NoUserData)
    }
    else {

      // TODO: fetch from the model

      Left(List(
        Video("youtube.com/watch?v=EWSRbRUH7pg"),
        Video("youtube.com/watch?v=MEgyGamo79I"),
        Video("youtube.com/watch?v=sPiWg5jSoZI"),
        Video("youtube.com/watch?v=0jEmxRJoAtQ")
      ))
    }
  }
}
