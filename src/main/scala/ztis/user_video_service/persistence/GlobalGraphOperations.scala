package ztis.user_video_service.persistence

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.neo4j.graphdb.GraphDatabaseService


object GlobalGraphOperations extends UnitOfWork with StrictLogging {
  def cleanDatabase(db: GraphDatabaseService): Unit = {
    logger.info("Cleaning database")

    implicit val service = db

    unitOfWork(() => service.execute("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"))
  }

  def initializeDatabase(db: GraphDatabaseService,
                         schemaInitializer: SchemaInitializer): Unit = {
    logger.info("Initializing database")

    implicit val service = db
    schemaInitializer.initialize()
  }
}
