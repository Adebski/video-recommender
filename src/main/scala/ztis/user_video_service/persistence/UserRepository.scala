package ztis.user_video_service.persistence

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import ztis.user_video_service.FieldNames
import ztis.user_video_service.UserServiceActor.{RegisterTwitterUser, RegisterWykopUser, TwitterUserRegistered, WykopUserRegistered}

/**
 * Methods in this class assume that they are executed inside already running transaction
 * @param graphDatabaseService
 */
class UserRepository(graphDatabaseService: GraphDatabaseService) extends StrictLogging {

  implicit val _service = graphDatabaseService

  def getOrCreateTwitterUser(request: RegisterTwitterUser, nextInternalID: Int): (Int, TwitterUserRegistered) = {
    logger.debug(s"getOrCreateTwitterUser request = $request, nextInternalID = $nextInternalID")
    val index = Indexes.TwitterUserExternalUserID

    val node = Option(graphDatabaseService.findNode(index.label, index.property, request.externalUserID))
      .getOrElse(createTwitterUser(index, request, nextInternalID))
    val internalUserID = node.getProperty(FieldNames.InternalUserID).asInstanceOf[Int]     
    val updatedNextInternalID = if (internalUserID == nextInternalID) {
      nextInternalID +1
    } else {
      nextInternalID
    }
    
    (updatedNextInternalID, TwitterUserRegistered(internalUserID, request))
  }

  private def createTwitterUser(index: IndexDefinition, request: RegisterTwitterUser, internalID: Int): Node = {
    val node = graphDatabaseService.createNode(index.label)

    node.setProperty(FieldNames.ExternalUserID, request.externalUserID)
    node.setProperty(FieldNames.ExternalUserName, request.externalUserName)
    node.setProperty(FieldNames.InternalUserID, internalID)

    node
  }

  def getOrCreateWykopUser(request: RegisterWykopUser, nextInternalID: Int): (Int, WykopUserRegistered) = {
    logger.debug(s"getOrCreateWykopUser $request")
    val index = Indexes.WykopUserExternalUserName

    val node = Option(graphDatabaseService.findNode(index.label, index.property, request.externalUserName))
      .getOrElse(createWykopUser(index, request, nextInternalID))
    val internalUserID = node.getProperty(FieldNames.InternalUserID).asInstanceOf[Int]
    val updatedNextInternalID = if (internalUserID == nextInternalID) {
      nextInternalID +1
    } else {
      nextInternalID
    }
    
    (updatedNextInternalID, WykopUserRegistered(internalUserID, request))
  }

  private def createWykopUser(index: IndexDefinition, request: RegisterWykopUser, internalID: Int): Node = {
    val node = graphDatabaseService.createNode(index.label)

    node.setProperty(FieldNames.ExternalUserName, request.externalUserName)
    node.setProperty(FieldNames.InternalUserID, internalID)

    node
  }
}
