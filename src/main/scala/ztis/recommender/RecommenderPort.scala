package ztis.recommender

import akka.actor.{Props, ActorRef, Actor}
import spray.httpx.marshalling.ToResponseMarshallable
import spray.routing._
import spray.http._
import MediaTypes._

object RecommenderPortActor {
  def props(idMappingService : ActorRef) = {
     Props(classOf[RecommenderPortActor], idMappingService)
  }
}

class RecommenderPortActor(userVideoQueryService : ActorRef) extends Actor with RecommenderPort {
  def actorRefFactory = context
  def receive = runRoute(myRoute)

  private val mappingService : MappingService = new UserVideoQueryMappingService(userVideoQueryService)
  override val recommenderService: RecommenderService = new RecommenderService(mappingService)
}

trait RecommenderPort extends HttpService {
  val recommenderService: RecommenderService

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
        parameters(('twitterId.as[String], 'wykopId.as[String])).as(RecommendRequest) { request =>
          val recommendations = recommenderService.recommend(request)

          respondWithMediaType(`text/html`) {
            complete {
              if(recommendations.isLeft) {
                presentRecommendations(request, recommendations.left.get)
              }
              else {
                presentNoUserData(request)
              }
            }
          }
        }
      }
    }

  private def presentRecommendations(request: RecommendRequest, recommendations: Vector[Video]) = {
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
              for {recommendation <- recommendations}
                yield toXml(recommendation)
            }
          </ul>
        </div>
      </body>
    </html>
  }

  private def toXml(video: Video) = {
    <li>
      <a href={ video.url }>{video.url}</a>
    </li>
  }

  private def presentNoUserData(request: RecommendRequest) = {
    <html>
      <body>
        <h1>User</h1>
        <div>
          Twitter: {request.twitterId}, Wykop: {request.wykopId}
        </div>
        <h2>
          We haven't got any data about the given user. We will try to fetch it soon.
          Please visit our service tomorrow.
        </h2>
      </body>
    </html>
  }
}