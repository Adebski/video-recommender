package ztis

/**
 * Represents user, followedUser, video association - one of the users I am following/observing rated given video.
 * 
 * Those are needed for representing the situation when one of our friends/followers rate some video. Those have to be separate from
 * normal ratings because we want to achieve idempotency for Cassandra updates. 
 * 
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
