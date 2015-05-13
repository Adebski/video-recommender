package ztis.user_video_service.persistence

import org.neo4j.graphdb.{Node, GraphDatabaseService}

/**
 * Methods in this class assume that they are executed inside already running transaction
 * @param graphDatabaseService
 */
class MetadataRepository(graphDatabaseService: GraphDatabaseService) {

  /**
   * Used for looking up metadata node - changing the value of this field will not be backwards compatible
   */
  private val lookupFieldValue = 1
  
  private val lookupIndex = Indexes.MetadataLookupField
  
  private def getOrCreateNode(): Node = {
    Option(graphDatabaseService.findNode(lookupIndex.label, lookupIndex.property, lookupFieldValue)).getOrElse {
      val node = graphDatabaseService.createNode(lookupIndex.label)

      node.setProperty(Indexes.NextInternalID, 0) 
      node.setProperty(lookupIndex.property, lookupFieldValue)
      
      node
    }
  }
  
  def metadata: Metadata = {
    val node = getOrCreateNode()
    
    Metadata(node.getProperty(Indexes.NextInternalID).asInstanceOf[Int])
  }
  
  def updateNextInternalID(newNextInternalID: Int) = {
    val node = getOrCreateNode()
    node.setProperty(Indexes.NextInternalID, newNextInternalID)
  }
}
