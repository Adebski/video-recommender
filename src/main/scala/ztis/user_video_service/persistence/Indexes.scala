package ztis.user_video_service.persistence

import ztis.user_video_service.FieldNames

object Indexes {
  val TwitterUserExternalUserID = IndexDefinition(Labels.TwitterUser, FieldNames.ExternalUserID)
  val TwitterUserExternalUserName = IndexDefinition(Labels.TwitterUser, FieldNames.ExternalUserName)
  val TwitterUserInternalUserID = IndexDefinition(Labels.TwitterUser, FieldNames.InternalUserID)
  val WykopUserExternalUserName = IndexDefinition(Labels.WykopUser, FieldNames.ExternalUserName)
  val WykopUserInternalUserID = IndexDefinition(Labels.WykopUser, FieldNames.InternalUserID)
  val NextUserInternalIDNodeLookupField = IndexDefinition(Labels.NextUserInternalID, FieldNames.LookupField)
  val NextVideoInternalIDNodeLookupField = IndexDefinition(Labels.NextVideoInternalID, FieldNames.LookupField)
  val VideoOrigin = IndexDefinition(Labels.Video, FieldNames.VideoOrigin)
  val VideoLink = IndexDefinition(Labels.Video, FieldNames.VideoLink)
  val VideoInternalID = IndexDefinition(Labels.Video, FieldNames.InternalVideoID)
  
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
