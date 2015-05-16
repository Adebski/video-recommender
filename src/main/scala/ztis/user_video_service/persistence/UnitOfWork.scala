package ztis.user_video_service.persistence

import org.neo4j.graphdb.GraphDatabaseService
import ztis.user_video_service.UnitOfWorkHelper


trait UnitOfWork {
  def unitOfWork[T](action: () => T)(implicit graphDatabaseService: GraphDatabaseService): T = {
    UnitOfWorkHelper.performTransactionally(action, graphDatabaseService)
  }
}
