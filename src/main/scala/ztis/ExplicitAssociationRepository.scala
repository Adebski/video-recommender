package ztis

import com.datastax.driver.core.Row

import scala.collection.JavaConverters._

class ExplicitAssociationRepository(client: CassandraClient) {
  
  def getAllAssociations(): Vector[ExplicitAssociationEntry] = {
    val resultSet = client.getAllExplicitAssociationRows()
    val iterator = resultSet.iterator().asScala
    
    iterator.map(toExplicitAssociationEntry).toVector
  }
  
  private def toExplicitAssociationEntry(row: Row): ExplicitAssociationEntry = {
    val userName = row.getString("user_id")
    val origin = toUserOrigin(row.getString("user_origin"))
    val link = row.getString("link")
    val rating = row.getInt("rating")
    
    ExplicitAssociationEntry(userName, origin, link, rating)
  }
  
  private def toUserOrigin(origin: String): UserOrigin = {
    if (origin == "twitter") {
      UserOrigin.Twitter
    } else {
      throw new IllegalArgumentException(s"Unrecognized origin $origin")
    }
  }
}
