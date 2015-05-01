package ztis.recommender

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}
import com.datastax.spark.connector._

import org.apache.spark.SparkContext._
import scala.collection.JavaConverters._

class RecommenderService extends StrictLogging {
  private val model : MatrixFactorizationModel = fetchModel()
  logger.info("Recommendation model successfully loaded.")

  def scheduleUserDataRetrieval(request: RecommendRequest) = {
    // TODO: put to the queue

    logger.info(s"Data not found. Scheduled twitter user ${request.twitterId} for high priority data retrieval")
    logger.info(s"Data not found. Scheduled wykop user ${request.wykopId} for high priority data retrieval")
  }

  def recommend(request: RecommendRequest) : Either[List[Video], NoUserData.type] = {
    // TODO: get both twitter and wykop ids

    val userId = request.twitterId.toInt

    val recommendation: Array[Rating] = model.recommendProducts(userId, 3)

    if(recommendation.isEmpty) { // TODO: change the condition
      scheduleUserDataRetrieval(request)
      Right(NoUserData)
    }
    else {
      Left(recommendation.map(rating => Video(rating.product.toString)).toList)
    }
  }

  private def fetchModel() : MatrixFactorizationModel = {
    val config = ConfigFactory.load("model")
    val sparkConfig = setupSpark(config)
    val spark = new SparkContext(sparkConfig)

    val keyspace = config.getString("cassandra.keyspace")
    val userFeatureTable = config.getString("model.user-feature-table")
    val productFeatureTable = config.getString("model.product-feature-table")

    val userFeatures = loadFeatureRDD(spark, keyspace, userFeatureTable)
    val productFeatures = loadFeatureRDD(spark, keyspace, productFeatureTable)

    userFeatures.cache()
    productFeatures.cache()

    val rank = userFeatures.first()._2.length

    new MatrixFactorizationModel(rank, userFeatures, productFeatures)
  }

  private def loadFeatureRDD(spark: SparkContext, keyspace: String, tableName: String): RDD[(Int, Array[Double])] = {
    spark.cassandraTable[(Int, List[Double])](keyspace, tableName)
         .map(feature => (feature._1, feature._2.toArray))
  }


  // TODO(#8): copied from MovieLensDataLoader, adhere to DRY,
  private def setupSpark(config: Config) = {
    val cassandraHost = config.getStringList("cassandra.contact-points").asScala.iterator.next().split(":")(0)

    val sparkConfig = new SparkConf()
      .setMaster(s"local[2]")
      .setAppName("ModelBuilder")
      .set("spark.executor.memory", "8g")
      .set("spark.cassandra.connection.host", cassandraHost)

    sparkConfig
  }
}
