package ztis.dump

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis._
import ztis.cassandra.{CassandraClient, SparkCassandraClient, CassandraConfiguration}

class CsvDumper extends Initializer with StrictLogging {
  override def initialize(): Unit = {
    val config = ConfigFactory.load("cassandra")
    val cassandraConfig = CassandraConfiguration(config)
    val sparkConfig = SparkCassandraClient.setCassandraConfig(Spark.baseConfiguration("CsvDumper"), cassandraConfig)
    val sparkCassandraClient = new SparkCassandraClient(new CassandraClient(cassandraConfig), Spark.sparkContext(sparkConfig))

    val ratings = sparkCassandraClient.userVideoRatingsRDD
    val relationships = sparkCassandraClient.userVideoImplicitAssociations
    val fullRatings = sparkCassandraClient.userVideoFullInformation

    ratings.cache()
    relationships.cache()
    fullRatings.cache()

    ratings.map {
      case UserVideoRating(userID, userOrigin, videoID, videoOrigin, rating) => {
        s"$userID,$userOrigin,$videoID,$videoOrigin,$rating"
      }
    }.saveAsTextFile("user-video-ratings.csv")

    relationships.map {
      case UserVideoImplicitAssociation(internalUserID, userOrigin, followedInternalUserID, followedUserOrigin, internalVideoID, videoOrigin) => {
        s"$internalUserID,$userOrigin,$followedInternalUserID,$followedUserOrigin,$internalVideoID,$videoOrigin"
      }
    }.saveAsTextFile("user-video-implicit-associatios.csv")

    fullRatings.map {
      case UserVideoFullInformation(userID, userOrigin, videoID, videoOrigin, rating, timesRatedByFollowedUsers) => {
        s"$userID,$userOrigin,$videoID,$videoOrigin,$rating,$timesRatedByFollowedUsers"
      }
    }.saveAsTextFile("user-video-full-information.csv")


    val ratingsCount = ratings.count()
    val wykopRatingsCount = ratings.filter(_.userOrigin == UserOrigin.Wykop).count()
    val twitterRatingsCount = ratings.filter(_.userOrigin == UserOrigin.Twitter).count()
    val youtubeRatingsCount = ratings.filter(_.videoOrigin == VideoOrigin.YouTube).count()
    val vimeoRatingsCount = ratings.filter(_.videoOrigin == VideoOrigin.Vimeo).count()
    val distinctUsers = ratings.map(_.userID).distinct().count()
    val distinctVideos = ratings.map(_.videoID).distinct().count()
    val averageNumberOfVideosPerUsers = ratingsCount.toDouble / distinctUsers
    val maxNumberOfVideosPerUser = ratings.groupBy(_.userID).map(_._2.size).max()

    val relationshipCount = relationships.count()

    val fullratingsCount = fullRatings.count()
    val fullwykopRatingsCount = fullRatings.filter(_.userOrigin == UserOrigin.Wykop).count()
    val fulltwitterRatingsCount = fullRatings.filter(_.userOrigin == UserOrigin.Twitter).count()
    val fullyoutubeRatingsCount = fullRatings.filter(_.videoOrigin == VideoOrigin.YouTube).count()
    val fullvimeoRatingsCount = fullRatings.filter(_.videoOrigin == VideoOrigin.Vimeo).count()
    val fulldistinctUsers = fullRatings.map(_.userID).distinct().count()
    val fulldistinctVideos = fullRatings.map(_.videoID).distinct().count()
    val fullaverageNumberOfVideosPerUsers = fullratingsCount.toDouble / fulldistinctUsers
    val fullmaxNumberOfVideosPerUser = fullRatings.groupBy(_.userID).map(_._2.size).max()

    val sumOfTimesRatedByFollowers = fullRatings.map(_.timesRatedByFollowedUsers).reduce(_ + _)
    val avgOfTimesRatedByFollowers = sumOfTimesRatedByFollowers.toDouble / fullratingsCount
    val maxOfTimesRatedByFollowers = fullRatings.map(_.timesRatedByFollowedUsers).max()

    val numberOfZeroRatings = fullRatings.filter(_.rating == 0).count()
    val numberOfZeroTimesRated = fullRatings.filter(_.timesRatedByFollowedUsers == 0).count()

    logger info s"ratings - count: $ratingsCount"
    logger info s"ratings - wykop ratings count: $wykopRatingsCount"
    logger info s"ratings - twitter count: $twitterRatingsCount"
    logger info s"ratings - youtube count: $youtubeRatingsCount"
    logger info s"ratings - vimeo count: $vimeoRatingsCount"
    logger info s"ratings - distinct number of users: $distinctUsers"
    logger info s"ratings - distinct number of videos: $distinctVideos"
    logger info s"ratings - average number of videos per user: $averageNumberOfVideosPerUsers"
    logger info s"ratings - max number of videos per user: $maxNumberOfVideosPerUser"

    logger info s"relationships count: $relationshipCount"

    logger info s"fullratings - count: $fullratingsCount"
    logger info s"fullratings - wykop ratings count: $fullwykopRatingsCount"
    logger info s"fullratings - twitter count: $fulltwitterRatingsCount"
    logger info s"fullratings - youtube count: $fullyoutubeRatingsCount"
    logger info s"fullratings - vimeo count: $fullvimeoRatingsCount"
    logger info s"fullratings - distinct number of users: $fulldistinctUsers"
    logger info s"fullratings - distinct number of videos: $fulldistinctVideos"
    logger info s"fullratings - average number of videos per user: $fullaverageNumberOfVideosPerUsers"
    logger info s"fullratings - max number of videos per user: $fullmaxNumberOfVideosPerUser"

    logger info s"fullratings - sum of times rated by followers: $sumOfTimesRatedByFollowers"
    logger info s"fullratings - avg of times rated by followers: $avgOfTimesRatedByFollowers"
    logger info s"fullratings - max of times rated by followers: $maxOfTimesRatedByFollowers"
    logger info s"fullratings - number of zero ratings: $numberOfZeroRatings"
    logger info s"fullratings - number of zero times rated by followers: $numberOfZeroTimesRated"
  }
}
