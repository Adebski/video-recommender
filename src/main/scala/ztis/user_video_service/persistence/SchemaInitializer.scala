package ztis.user_video_service.persistence

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.neo4j.graphdb.GraphDatabaseService

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class SchemaInitializer(graphDatabaseService: GraphDatabaseService,
                        waitDuration: Option[Duration] = None) extends StrictLogging with UnitOfWork {
  implicit val _service = graphDatabaseService

  def initialize(): Unit = {
    logger.info(s"Initializing indexes ${Indexes.definitions}")

    Indexes.definitions.foreach(createIndex)

    waitForIndexesToBeReady()
  }

  private def createIndex(index: IndexDefinition): Unit = {
    logger.info(s"Creating index $index")

    unitOfWork { () =>
      if (hasIndex(index)) {
        logger.warn(s"There is already index for $index}")
      }
      else {
        schema.indexFor(index.label).on(index.property).create()
      }
    }

    logger.info(s"Created index from $index")
  }

  private def hasIndex(index: IndexDefinition): Boolean = {
    val indexesForLabel = schema.getIndexes(index.label).asScala

    indexesForLabel.exists(_.getPropertyKeys.asScala.exists(_ == index.property))
  }

  private def schema = {
    graphDatabaseService.schema()
  }

  private def waitForIndexesToBeReady(): Unit = {
    logger.info(s"Waiting for indexes to be ready for $waitDuration")
    unitOfWork { () =>
      waitDuration.foreach { duration =>
        schema.awaitIndexesOnline(duration.toMillis, TimeUnit.MILLISECONDS)
      }
    }
  }
}
