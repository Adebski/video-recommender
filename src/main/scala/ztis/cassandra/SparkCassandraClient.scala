package ztis.cassandra

import com.datastax.spark.connector._
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ztis.{UserAndRating, UserOrigin}

class SparkCassandraClient(val client: CassandraClient, val sparkContext: SparkContext) extends StrictLogging {

  type FeaturesRDD = RDD[(Int, Array[Double])]

  private val columns = SomeColumns("user_id", "user_origin", "link", "rating", "timesUpvotedByFriends")

  def userAndRatingsRDD: RDD[UserAndRating] = {
    sparkContext.cassandraTable(client.keyspace, client.ratingsTableName).map { row =>
      val userId = row.getString("user_id")
      val origin = UserOrigin.fromString(row.getString("user_origin"))
      val link = row.getString("link")
      val rating = row.getInt("rating")
      val timesUpvotedByFriends = row.getInt("timesUpvotedByFriends")

      UserAndRating(userId, origin, link, rating, timesUpvotedByFriends)
    }
  }

  /*
  TODO - #16. Here we are assuming that username and link are represented as ints - that may not be true. 
  When the data is pulled from Wykop/Twitter links are represented as text and user ids may be textual (wykop) or 
  integers (twitter) - for now this will only work when data is pulled from movielens database. 
 */

  def ratingsRDD: RDD[Rating] = {
    sparkContext.cassandraTable(client.keyspace, client.ratingsTableName).map { row =>
      val userId = row.getInt("user_id")
      val link = row.getInt("link")
      val rating = row.getInt("rating")

      Rating(userId, link, rating)
    }  
  }
  
  def saveUserAndRatings(rdd: RDD[UserAndRating]): RDD[UserAndRating] = {
    rdd.map(_.toTuple).saveToCassandra(client.keyspace, client.ratingsTableName, columns)
    rdd
  }

  def saveModel(model: MatrixFactorizationModel): Unit = {
    saveFeatures(model.userFeatures, client.keyspace, client.userFeaturesTableName)
    saveFeatures(model.productFeatures, client.keyspace, client.productFeaturesTableName)
  }


  private def saveFeatures(rdd: FeaturesRDD, keyspace: String, table: String): Unit = {
    client.dropTable(keyspace, table)
    //toVector because spark connector does not support automatic mapping of mutable types
    rdd.map(feature => (feature._1, feature._2.toVector)).saveAsCassandraTable(keyspace, table)
  }
  
  def fetchModel : MatrixFactorizationModel = {
    val userFeatures = loadFeatures(client.keyspace, client.userFeaturesTableName)
    val productFeatures = loadFeatures(client.keyspace, client.productFeaturesTableName)

    userFeatures.cache()
    productFeatures.cache()

    val rank = userFeatures.first()._2.length

    new MatrixFactorizationModel(rank, userFeatures, productFeatures)
  }

  private def loadFeatures(keyspace: String, tableName: String): RDD[(Int, Array[Double])] = {
    sparkContext.cassandraTable[(Int, Vector[Double])](keyspace, tableName).map(feature => (feature._1, feature._2.toArray))
  }
}

object SparkCassandraClient {
  def setCassandraConfig(sparkConfig: SparkConf, appConfig: Config): SparkConf = {
    val contactPoint = CassandraClient.contactPoints(appConfig).get(0)

    sparkConfig.set("spark.cassandra.connection.host", contactPoint.getHostString)
    sparkConfig.set("spark.cassandra.connection.native.port", contactPoint.getPort.toString)

    sparkConfig
  }
}
