package ztis.user_video_service.persistence

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Node}
import ztis.twitter.TwitterUser
import ztis.user_video_service.FieldNames
import ztis.user_video_service.UserServiceActor._
import scala.collection.JavaConverters._

/**
 * Methods in this class assume that they are executed inside already running transaction
 * @param graphDatabaseService
 */
class UserRepository(graphDatabaseService: GraphDatabaseService) extends StrictLogging {

  implicit val _service = graphDatabaseService

  private val createRelationshipParamMap = new java.util.HashMap[String, Object]()

  def getOrCreateTwitterUser(request: RegisterTwitterUser, nextInternalID: Int): (Int, TwitterUserRegistered) = {
    logger.debug(s"getOrCreateTwitterUser request = $request, nextInternalID = $nextInternalID")
    val result = getOrCreateTwitterUser(request.externalUserID, request.externalUserName, nextInternalID)
    val twitterUserInternalID = result._2
    
    (result._1, TwitterUserRegistered(twitterUserInternalID, fetchFollowers(Indexes.TwitterUserInternalUserID, twitterUserInternalID), request))
  }

  private def getOrCreateTwitterUser(externalUserID: Long, externalUserName: String, nextInternalID: Int): (Int, Int) = {
    logger.debug(s"getOrCreateTwitterUser externalUserID = $externalUserID, externalUserName = $externalUserName, nextInternalID = $nextInternalID")
    val index = Indexes.TwitterUserExternalUserID

    val node = Option(getNodeOrNull(index, externalUserID))
      .getOrElse(createTwitterUser(externalUserID, externalUserName, nextInternalID))
    val internalUserID = internalID(node)
    val updatedNextInternalID = if (internalUserID == nextInternalID) {
      nextInternalID + 1
    } else {
      nextInternalID
    }

    (updatedNextInternalID, internalUserID)
  }

  def createRelationshipsToTwitterUser(externalUserID: Long,
                                       fromUsers: Vector[TwitterUser],
                                       nextInternalID: Int): (Int, ToTwitterUserRelationshipsCreated) = {
    var _nextInternalID = nextInternalID
    val toUserNode = getNodeOrNull(Indexes.TwitterUserExternalUserID, externalUserID)
    val toUserInternalID = internalID(toUserNode)
    val fromUsersInternalIDs = fromUsers.map { user =>
      val result = getOrCreateTwitterUser(user.userID, user.userName, _nextInternalID)
      _nextInternalID = result._1

      result._2
    }

    fromUsersInternalIDs.foreach(fromUserInternalID => createRelationshipTwitter(toUserInternalID, fromUserInternalID))

    (_nextInternalID, ToTwitterUserRelationshipsCreated(toUserInternalID, fromUsersInternalIDs))
  }

  private def createRelationshipTwitter(tuUserInternalID: java.lang.Integer, fromInternalUserID: java.lang.Integer): Unit = {
    createRelationshipParamMap.put("fromUserInternalID", fromInternalUserID)
    createRelationshipParamMap.put("toUserInternalID", tuUserInternalID)

    val result = graphDatabaseService.execute(UserRepository.CreateFollowsRelationshipTwitterQuery, createRelationshipParamMap)
    result.close()
  }

  def getTwitterUser(externalUserName: String): Option[Int] = {
    getInternalUserID(Indexes.TwitterUserExternalUserName, externalUserName)
  }

  def getWykopUser(externalUserName: String): Option[Int] = {
    getInternalUserID(Indexes.WykopUserExternalUserName, externalUserName)
  }

  private def getInternalUserID(lookupIndex: IndexDefinition, externalUserName: String): Option[Int] = {
    val node = Option(graphDatabaseService.findNode(lookupIndex.label, lookupIndex.property, externalUserName))

    node.map(_.getProperty(FieldNames.InternalUserID).asInstanceOf[Int])
  }

  private def getInternalUserID(lookupIndex: IndexDefinition, externalUserID: Long): Option[Int] = {
    val node = Option(graphDatabaseService.findNode(lookupIndex.label, lookupIndex.property, externalUserID))

    node.map(_.getProperty(FieldNames.InternalUserID).asInstanceOf[Int])
  }

  private def createTwitterUser(externalUserID: Long, externalUserName: String, internalID: Int): Node = {
    val node = graphDatabaseService.createNode(Labels.TwitterUser)

    node.setProperty(FieldNames.ExternalUserID, externalUserID)
    node.setProperty(FieldNames.ExternalUserName, externalUserName)
    node.setProperty(FieldNames.InternalUserID, internalID)

    node
  }

  private def getNodeOrNull(index: IndexDefinition, property: Int): Node = {
    graphDatabaseService.findNode(index.label, index.property, property)
  }

  private def getNodeOrNull(index: IndexDefinition, property: String): Node = {
    graphDatabaseService.findNode(index.label, index.property, property)
  }

  private def getNodeOrNull(index: IndexDefinition, property: Long): Node = {
    graphDatabaseService.findNode(index.label, index.property, property)
  }

  private def internalID(node: Node): Int = {
    node.getProperty(FieldNames.InternalUserID).asInstanceOf[Int]
  }

  def getOrCreateWykopUser(request: RegisterWykopUser, nextInternalID: Int): (Int, WykopUserRegistered) = {
    logger.debug(s"getOrCreateWykopUser $request")
    val index = Indexes.WykopUserExternalUserName

    val node = Option(getNodeOrNull(index, request.externalUserName))
      .getOrElse(createWykopUser(index, request, nextInternalID))
    val internalUserID = node.getProperty(FieldNames.InternalUserID).asInstanceOf[Int]
    val updatedNextInternalID = if (internalUserID == nextInternalID) {
      nextInternalID + 1
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
  
  private def fetchFollowers(index: IndexDefinition, internalUserID: Int): Vector[Int] = {
    val node = getNodeOrNull(index, internalUserID)
    
    val relationships = node.getRelationships(Relationships.FromFollowerToFollowedUser, Direction.INCOMING)
      .asScala.map(relationship => internalID(relationship.getStartNode)).toVector
    
    relationships
  }
}

object UserRepository {
  private val CreateFollowsRelationshipTwitterQuery =
    s"""
       |MATCH (fromUser: ${Labels.TwitterUser} { ${FieldNames.InternalUserID}: {fromUserInternalID}}),
       |(toUser: ${Labels.TwitterUser} { ${FieldNames.InternalUserID}: {toUserInternalID}})
       |CREATE UNIQUE (fromUser)-[:${Relationships.FromFollowerToFollowedUser}]->(toUser)  
     """.stripMargin
}
