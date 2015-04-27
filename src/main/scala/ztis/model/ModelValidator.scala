package ztis.model

import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating, ALS}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._

import scala.collection.JavaConverters._

object ModelValidator extends App with StrictLogging {
  val config = ConfigFactory.load("testdata")
  val spark = setupSpark(config)

  val associations = associationRdd.sample(withReplacement = false, fraction = 0.001)

  val ratings = associations.map(row => {
    // temporarily
    val userId = row.getString("user_id").toInt
    val link = row.getString("link").toInt
    val rating = row.getInt("rating")

    Rating(userId, link, rating.toDouble)
  })

  val Array(training, validation, test) = ratings.randomSplit(Array(0.6, 0.2, 0.2))

  training.cache()
  validation.cache()
  test.cache()

  println(s"training: ${training.count()}, validation: ${validation.count()}, test: ${test.count()}")

  val rank = 8
  val lambda = 0.1
  val numIters = 10

  val model = ALS.train(training, rank, numIters, lambda)
  val validationRmse = computeRmse(model, validation)

  println(s"Rmse: ${validationRmse}")

  spark.stop()

  private def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating)).join(
      data.map(x => ((x.user, x.product), x.rating))).values

    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / validation.count())
  }

  // extract to CassandraClient or sth like this
  private def associationRdd = {
    val keyspace = config.getString("cassandra.keyspace")
    val explicitAssocTableName = config.getString("cassandra.explicit-association-table-name")

    spark.cassandraTable(keyspace, explicitAssocTableName.toLowerCase)
  }





  // TODO: copied from MovieLensDataLoder, adhere to DRY,
  private def setupSpark(config: Config): SparkContext = {
    val cassandraHost = config.getStringList("cassandra.contact-points").asScala.iterator.next().split(":")(0)

    val sparkConfig = new SparkConf()
      .setMaster(s"local[2]")
      .setAppName("ModelValidator")
      .set("spark.executor.memory", "8g")
      .set("spark.cassandra.connection.host", cassandraHost)

    new SparkContext(sparkConfig)
  }

}
