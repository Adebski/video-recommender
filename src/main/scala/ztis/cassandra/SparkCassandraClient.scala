package ztis.cassandra

import com.datastax.spark.connector._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext._
import ztis._

class SparkCassandraClient(val client: CassandraClient, val sparkContext: SparkContext) extends StrictLogging {

  type FeaturesRDD = RDD[(Int, Array[Double])]

  private val columns = SomeColumns("user_id", "user_origin", "video_id", "video_origin", "rating")

  def userVideoRatingsRDD: RDD[UserVideoRating] = {
    sparkContext.cassandraTable(client.config.keyspace, client.config.ratingsTableName).map { row =>
      val userId = row.getInt("user_id")
      val userOrigin = UserOrigin.fromString(row.getString("user_origin"))
      val videoID = row.getInt("video_id")
      val videoOrigin = VideoOrigin.fromString(row.getString("video_origin"))
      val rating = row.getInt("rating")

      UserVideoRating(userId, userOrigin, videoID, videoOrigin, rating)
    }
  }

  def userVideoImplicitAssociations: RDD[UserVideoImplicitAssociation] = {
    sparkContext.cassandraTable(client.config.keyspace, client.config.relationshipsTableName).map { row =>
      val userId = row.getInt("user_id")
      val userOrigin = UserOrigin.fromString(row.getString("user_origin"))
      val followedUserId = row.getInt("followed_user_id")
      val followedUserOrigin = UserOrigin.fromString(row.getString("followed_user_origin"))
      val videoID = row.getInt("video_id")
      val videoOrigin = VideoOrigin.fromString(row.getString("video_origin"))

      UserVideoImplicitAssociation(userId, userOrigin, followedUserId, followedUserOrigin, videoID, videoOrigin)
    }  
  }
  
  def userVideoFullInformation: RDD[UserVideoFullInformation] = {
    val userVideoWithFollowersRated = sparkContext.cassandraTable(client.config.keyspace, client.config.relationshipsTableName).keyBy(row => {
      (row.getInt("user_id"), row.getString("user_origin"), row.getInt("video_id"), row.getString("video_origin"))
    }).spanByKey.map(userVideoWithFollowed => (userVideoWithFollowed._1, userVideoWithFollowed._2.size))
    
    val userVideoRatings = sparkContext.cassandraTable(client.config.keyspace, client.config.ratingsTableName).map { row =>
      val userId = row.getInt("user_id")
      val userOrigin = row.getString("user_origin")
      val videoID = row.getInt("video_id")
      val videoOrigin = row.getString("video_origin")
      val rating = row.getInt("rating")

      ((userId, userOrigin, videoID, videoOrigin), rating)
    } 
    
    userVideoRatings.fullOuterJoin(userVideoWithFollowersRated).map { userVideoWithAllInformation =>
      val key = userVideoWithAllInformation._1
      val rating = userVideoWithAllInformation._2._1
      val timesRatedByFollowedUsers = userVideoWithAllInformation._2._2
      
      UserVideoFullInformation(userID = key._1,
        userOrigin = UserOrigin.fromString(key._2),
        videoID = key._3,
        videoOrigin = VideoOrigin.fromString(key._4), 
        rating = rating.getOrElse(0),
        timesRatedByFollowedUsers = timesRatedByFollowedUsers.getOrElse(0))  
    }
  }
  
  def ratingsRDD: RDD[Rating] = {
    sparkContext.cassandraTable(client.config.keyspace, client.config.ratingsTableName).map { row =>
      val userId = row.getInt("user_id")
      val itemId = row.getInt("video_id")
      val rating = row.getInt("rating")

      Rating(userId, itemId, rating)
    }
  }

  def saveUserVideoRatings(rdd: RDD[UserVideoRating]): RDD[UserVideoRating] = {
    rdd.map(_.toTuple).saveToCassandra(client.config.keyspace, client.config.ratingsTableName, columns)
    rdd
  }

  def saveModel(model: MatrixFactorizationModel): Unit = {
    saveFeatures(model.userFeatures, client.config.keyspace, client.config.userFeaturesTableName)
    saveFeatures(model.productFeatures, client.config.keyspace, client.config.productFeaturesTableName)
  }


  private def saveFeatures(rdd: FeaturesRDD, keyspace: String, table: String): Unit = {
    client.dropTable(keyspace, table)
    //toVector because spark connector does not support automatic mapping of mutable types
    rdd.map(feature => (feature._1, feature._2.toVector)).saveAsCassandraTable(keyspace, table)
  }

  def fetchModel: MatrixFactorizationModel = {
    val userFeatures = loadFeatures(client.config.keyspace, client.config.userFeaturesTableName)
    val productFeatures = loadFeatures(client.config.keyspace, client.config.productFeaturesTableName)

    userFeatures.cache()
    productFeatures.cache()

    val rank = userFeatures.first()._2.length

    new MatrixFactorizationModel(rank, userFeatures, productFeatures)
  }

  private def loadFeatures(keyspace: String, tableName: String): RDD[(Int, Array[Double])] = {
    sparkContext.cassandraTable[(Int, Vector[Double])](keyspace, tableName).map(feature => (feature._1, feature._2.toArray))
  }
  
  def stop(): Unit = {
    sparkContext.stop()
    client.shutdown()
  }
}

object SparkCassandraClient {
  def setCassandraConfig(sparkConfig: SparkConf, cassandraConfiguration: CassandraConfiguration): SparkConf = {
    val contactPoint = cassandraConfiguration.contactPoints.get(0)

    sparkConfig.set("spark.cassandra.connection.host", contactPoint.getHostString)
    sparkConfig.set("spark.cassandra.connection.native.port", contactPoint.getPort.toString)

    sparkConfig
  }
}
