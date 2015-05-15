package ztis.user_video_service.persistence

import ztis.user_video_service.FieldNames

object Indexes {
  val TwitterUserExternalUserID = IndexDefinition("TwitterUser", FieldNames.ExternalUserID)
  val TwitterUserExternalUserName = IndexDefinition("TwitterUser", FieldNames.ExternalUserName)
  val TwitterUserInternalUserID = IndexDefinition("TwitterUser", FieldNames.InternalUserID)
  val WykopUserExternalUserName = IndexDefinition("WykopUser", FieldNames.ExternalUserName)
  val WykopUserInternalUserID = IndexDefinition("WykopUser", FieldNames.InternalUserID)
  val NextUserInternalIDNodeLookupField = IndexDefinition("nextUserInternalID", FieldNames.LookupField)
  val NextVideoInternalIDNodeLookupField = IndexDefinition("nextVideoInternalID", FieldNames.LookupField)
  val VideoOrigin = IndexDefinition("Video", FieldNames.VideoOrigin)
  val VideoLink = IndexDefinition("Video", FieldNames.VideoLink)
  val VideoInternalID = IndexDefinition("Video", FieldNames.InternalVideoID)
  
  val definitions: Vector[IndexDefinition] = Vector(
    TwitterUserExternalUserID,
    TwitterUserExternalUserName,
    TwitterUserInternalUserID,
    WykopUserExternalUserName,
    WykopUserInternalUserID,
    NextUserInternalIDNodeLookupField,
    NextVideoInternalIDNodeLookupField,
    VideoOrigin,
    VideoLink,
    VideoInternalID
  )
}
