package ztis.user_video_service.persistence

object Indexes {
  
  val ExternalUserID = "externalUserID"
  val ExternalUserName = "externalUserName"
  val InternalUserID = "internalUserID"
  val LookupField = "lookupField"
  val NextInternalID = "nextInternalID"
  
  val TwitterUserExternalUserID = IndexDefinition("TwitterUser", ExternalUserID)
  val TwitterUserExternalUserName = IndexDefinition("TwitterUser", ExternalUserName)
  val TwitterUserInternalUserID = IndexDefinition("TwitterUser", InternalUserID)
  val WykopUserExternalUserName = IndexDefinition("WykopUser", ExternalUserName)
  val WykopUserInternalUserID = IndexDefinition("WykopUser", InternalUserID)
  val MetadataLookupField = IndexDefinition("Metadata", LookupField)
  
  val definitions: Vector[IndexDefinition] = Vector(
    TwitterUserExternalUserID,
    TwitterUserExternalUserName,
    TwitterUserInternalUserID,
    WykopUserExternalUserName,
    WykopUserInternalUserID,
    MetadataLookupField
  )
}
