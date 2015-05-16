package ztis.user_video_service.persistence

import org.neo4j.graphdb.{GraphDatabaseService, Node}
import ztis.user_video_service.FieldNames

/**
 * Methods in this class assume that they are executed inside already running transaction
 * @param graphDatabaseService
 */
class MetadataRepository(graphDatabaseService: GraphDatabaseService) {

  /**
   * Used for looking up metadata node - changing the value of this field will not be backwards compatible
   */
  private val nextUserInternalIDLookupFieldValue = 1

  private val nextVideoInternalIDLookupFieldValue = 1

  private def getOrCreateNode(lookupIndex: IndexDefinition, lookupFieldValue: Int, internalIDFieldName: String): Node = {
    Option(graphDatabaseService.findNode(lookupIndex.label, lookupIndex.property, lookupFieldValue)).getOrElse {
      val node = graphDatabaseService.createNode(lookupIndex.label)

      node.setProperty(internalIDFieldName, 0)
      node.setProperty(lookupIndex.property, lookupFieldValue)

      node
    }
  }

  private def nextUserInternalIDValue(): Int = {
    val node = nextUserInternalIDNode()

    node.getProperty(FieldNames.NextUserInternalID).asInstanceOf[Int]
  }

  private def nextUserInternalIDNode(): Node = {
    getOrCreateNode(Indexes.NextUserInternalIDNodeLookupField, nextUserInternalIDLookupFieldValue, FieldNames.NextUserInternalID)
  }

  private def nextVideoInternalIDValue(): Int = {
    val node = nextVideoInternalIDNode()

    node.getProperty(FieldNames.NextVideoInternalID).asInstanceOf[Int]
  }

  private def nextVideoInternalIDNode(): Node = {
    getOrCreateNode(Indexes.NextVideoInternalIDNodeLookupField, nextVideoInternalIDLookupFieldValue, FieldNames.NextVideoInternalID)
  }

  def metadata: Metadata = {
    Metadata(nextUserInternalID = nextUserInternalIDValue(), nextVideoInternalID = nextVideoInternalIDValue())
  }

  def updateNextUserInternalID(newNextInternalID: Int) = {
    val node = nextUserInternalIDNode()

    updateNextInternalID(node, FieldNames.NextUserInternalID, newNextInternalID)
  }

  def updateNextVideoInternalID(newNextInternalID: Int) = {
    val node = nextVideoInternalIDNode()

    updateNextInternalID(node, FieldNames.NextVideoInternalID, newNextInternalID)
  }

  private def updateNextInternalID(node: Node, fieldName: String, value: Int): Unit = {
    node.setProperty(fieldName, value)
  }
}
