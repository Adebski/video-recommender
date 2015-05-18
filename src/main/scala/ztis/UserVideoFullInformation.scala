package ztis

/**
 * Represents full information about user, video association.
 * 
 * @param userID
 * @param userOrigin
 * @param videoID
 * @param videoOrigin
 * @param rating 0 - not rated, >0 - rated
 * @param numberOfFriendsLiking
 */
case class UserVideoFullInformation(userID: Int,
                                    userOrigin: UserOrigin,
                                    videoID: Int,
                                    videoOrigin: VideoOrigin,
                                    rating: Int,
                                    numberOfFriendsLiking: Int)
