package ztis.relationships

import akka.actor.{Actor, ActorLogging, Stash}
import ztis.UserOrigin
import ztis.cassandra.SparkCassandraClient

import scala.concurrent.Future
import scala.concurrent.duration._

trait RelationshipFetcher extends Actor with ActorLogging with Stash {
  def sparkJobForUpdating(sparkCassandra: SparkCassandraClient,
                          internalUserID: Int,
                          userOrigin: UserOrigin,
                          fromUsers: Vector[Int]): Unit = {
    import context.dispatcher
    val f = Future {
      sparkCassandra.updateMoviesForNewRelationships(internalUserID, userOrigin, fromUsers, userOrigin)
    }
    f.onComplete {
      case scala.util.Success(_) => {
        log.info(s"Updated relationships, to user $internalUserID, from users $fromUsers, origin $userOrigin")
      }
      case scala.util.Failure(e) => {
        log.error(e, s"Error updating relationships, to user $internalUserID, from users $fromUsers, origin $userOrigin")
      }
    }
  }

  def scheduleMessageAndStashCurrent(message: AnyRef, seconds: Int): Unit = {
    import context.dispatcher
    stash()
    Some(context.system.scheduler.scheduleOnce(seconds.seconds, self, message))
  }
}
