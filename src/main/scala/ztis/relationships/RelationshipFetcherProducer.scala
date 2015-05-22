package ztis.relationships

trait RelationshipFetcherProducer {
  def requestRelationshipsForWykopUser(wykopUserID: String): Unit
  
  def requestRelationshipsForTwitterUser(twitterUserID: Long): Unit
}
