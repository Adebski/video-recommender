package ztis.recommender

import akka.actor.Actor
import spray.routing._
import spray.http._
import MediaTypes._

class RecommenderServiceActor extends Actor with RecommenderService {
  def actorRefFactory = context
  def receive = runRoute(myRoute)
}

case class RecommendRequest(twitterId: String, wykopId: String)

trait RecommenderService extends HttpService {
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
          respondWithMediaType(`text/html`) {
            complete {
              <html>
                <body>
                  Twitter: {request.twitterId}, Wykop: {request.wykopId}
                </body>
              </html>
            }
          }
        }
      }
    }
}