package ztis

/**
 * Represents user, followedUser, video association - one of the users I am following/observing rated given video.
 * @param internalUserID
 * @param userOrigin
 * @param followedInternalUserID
 * @param followedUserOrigin
 * @param internalVideoID
 * @param videoOrigin
 */
case class UserVideoImplicitAssociation(internalUserID: Int,
                                        userOrigin: UserOrigin,
                                        followedInternalUserID: Int,
                                        followedUserOrigin: UserOrigin,
                                        internalVideoID: Int,
                                        videoOrigin: VideoOrigin)
