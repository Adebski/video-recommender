package ztis.model

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating, ALS}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.reflect.io.File
import scala.language.postfixOps
import scala.sys.process._
import scala.collection.JavaConverters._

object ModelBuilder extends App with StrictLogging {
  val config = ConfigFactory.load("model")
  val sparkConfig = setupSpark(config)
  val spark = new SparkContext(sparkConfig)

  val ratings = associationRdd.map(row => {
    // TODO(#16)
    val userId = row.getString("user_id").toInt
    val link = row.getString("link").toInt
    val rating = row.getInt("rating")

    Rating(userId, link, rating.toDouble)
  })

  val Array(training, validation, test) = ratings.randomSplit(Array(0.6, 0.2, 0.2))

  training.cache()
  validation.cache()
  test.cache()

  {
    val ranks = config.getIntList("model.params.ranks").asScala.toVector.map(_.toInt)
    val lambdas = config.getDoubleList("model.params.lambdas").asScala.toVector.map(_.toDouble)
    val numIters = config.getIntList("model.params.iterations").asScala.toVector.map(_.toInt)

    val directory = s"report-${DateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd--HH-mm-ss"))}"
    s"mkdir $directory" !!

    val paramsCrossProduct = for {rank <- ranks; lambda <- lambdas; numIter <- numIters} yield (rank, lambda, numIter)

    val results = paramsCrossProduct.map {
      case params@(rank, lambda, numIter) => {
        val modelDirectory = s"$directory/r$rank-l${(lambda * 100).toInt}-i$numIter"
        val (model, buildTime) = time {
          ALS.train(training, rank, numIter, lambda)
        }
        val score = Evaluations.evaluateAndGiveAUC(new ALSPrediction(model), validation, modelDirectory, buildTime)
        (score, model, params, buildTime)
      }
    }.sortBy(-_._1).toList

    val best = results.head
    val (score, model, (rank, lambda, numIter), _) = best

    unpersistModels(results.tail)
    dumpScores(results, directory + "/scores.txt")

    logger.info(s"The best model params: rank=$rank, lambda=$lambda, numIter=$numIter. It's ROC AUC = $score")

    val testSetScore =  Evaluations.evaluateAndGiveAUC(new ALSPrediction(model), test, directory + "/best-on-test-set")
    logger.info(s"ROC AUC of best model on test set = $testSetScore")

    val noPersonalizationPredictor = new NoPersonalizationPrediction(training, defaultRank = 2.5)
    val baselineScore = Evaluations.evaluateAndGiveAUC(noPersonalizationPredictor, validation, directory + "/baseline-product-mean")
    logger.info(s"baseline ROC AUC (no personalization) = $baselineScore")

    val randomPredictor = new RandomPrediction(training)
    val randomScore = Evaluations.evaluateAndGiveAUC(randomPredictor, validation, directory + "/baseline-random")
    logger.info(s"baseline ROC AUC (random) = $randomScore")

    persistInDatabase(model)
  }

  def unpersistModels(results: List[(Double, MatrixFactorizationModel, (Int, Double, Int), Double)]): Unit = {
    results.foreach {
      case (_, model, _, _) => {
        model.userFeatures.unpersist()
        model.productFeatures.unpersist()
      }
    }
  }

  spark.stop()

  def dumpScores(scores : List[(Double, MatrixFactorizationModel, (Int, Double, Int), Double)], filename: String) = {
    val header = "rank,lambda,numIter,score,time\n"
    val csv = scores.map {
      case (score, _, params, buildTime) => params.productIterator.toList.mkString(",") + "," + score.toString + "," + buildTime.toString
    }.mkString("\n")

    File(filename).writeAll(header, csv)
  }

  private def persistInDatabase(model: MatrixFactorizationModel) = {
    val keyspace = config.getString("cassandra.keyspace")
    val userFeaturesTable = config.getString("model.user-feature-table")
    val productFeaturesTable = config.getString("model.product-feature-table")

    saveFeatureRdd(keyspace, userFeaturesTable, model.userFeatures)
    saveFeatureRdd(keyspace, productFeaturesTable, model.productFeatures)
  }

  private def saveFeatureRdd(keyspace: String, tableName: String, rdd: RDD[(Int, Array[Double])]) = {
    CassandraConnector(sparkConfig).withSessionDo { session =>
      session.execute(s""" DROP TABLE IF EXISTS $keyspace."$tableName" """)
    }

    rdd.map(feature => (feature._1, feature._2.toList)).saveAsCassandraTable(keyspace, tableName)
  }

  // TODO(#8): extract to CassandraClient or sth like this
  private def associationRdd = {
    val keyspace = config.getString("cassandra.keyspace")
    val explicitAssocTableName = config.getString("cassandra.explicit-association-table-name")

    spark.cassandraTable(keyspace, explicitAssocTableName.toLowerCase)
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

  private def time[A](f: => A): (A, Double) = {
    val start = System.nanoTime
    val result = f
    val elapsedTimeInMs = (System.nanoTime-start)/1e6
    (result, elapsedTimeInMs)
  }
}
