package ztis.recommender

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._

class RecommenderPortActor extends Actor with RecommenderPort {
  def actorRefFactory = context
  def receive = runRoute(myRoute)

  override val recommenderService: RecommenderService = new RecommenderService
}



trait RecommenderPort extends HttpService {
  val recommenderService: RecommenderService

  private def toXml(video: Video) = {
    <li>
      <a href={ "http://" + video.url }>{video.url}</a>
    </li>
  }

  val myRoute =
    path("") {
      get {
        respondWithMediaType(`text/html`) {
          complete {
            <html>
              <body>
                <form action="/recommend" method="get"> 
                  <label class="description" for="element_1">Twitter username </label>
                  <div>
                    <input id="element_1" name="twitterId" class="element text medium" type="text" maxlength="255" value=""/> 
                  </div> 
                  <label class="description" for="element_2">Wykop.pl username </label>
                  <div>
                    <input id="element_2" name="wykopId" class="element text medium" type="text" maxlength="255" value=""/> 
                  </div> 
                  <input id="saveForm" class="button_text" type="submit" />
                </form>
              </body>
            </html>
          }
        }
      }
    } ~ path("recommend") {
      get {
        parameters('twitterId.as[String], 'wykopId.as[String]).as(RecommendRequest) { request =>
          val recommendations = recommenderService.recommend(request)

          respondWithMediaType(`text/html`) {
            complete {
              <html>
                <body>
                  <h1>User</h1>
                  <div>
                    Twitter: {request.twitterId}, Wykop: {request.wykopId}
                  </div>
                  <h1>Recommendations:</h1>
                  <div>
                    <ul>
                      {
                        for { recommendation <- recommendations }
                          yield toXml(recommendation)
                      }
                    </ul>
                  </div>
                </body>
              </html>
            }
          }
        }
      }
    }
}