package ztis.recommender

class RecommenderService {
  def recommend(request: RecommendRequest) : List[Video] = {
    List(
      Video("youtube.com/watch?v=EWSRbRUH7pg"),
      Video("youtube.com/watch?v=MEgyGamo79I"),
      Video("youtube.com/watch?v=sPiWg5jSoZI"),
      Video("youtube.com/watch?v=0jEmxRJoAtQ")
    )
  }
}
