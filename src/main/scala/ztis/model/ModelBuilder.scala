package ztis.model

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import ztis.Spark
import ztis.cassandra.{CassandraClient, SparkCassandraClient}

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.reflect.io.File
import scala.sys.process._

object ModelBuilder extends App with StrictLogging {
  val config = ConfigFactory.load("model")
  val sparkConfig = SparkCassandraClient.setCassandraConfig(Spark.baseConfiguration("ModelBuilder"), config)
  val sparkCassandraClient = new SparkCassandraClient(new CassandraClient(config), Spark.sparkContext(sparkConfig))
  val ratings = sparkCassandraClient.ratingsRDD

  val Array(training, validation, test) = ratings.randomSplit(Array(0.6, 0.2, 0.2))

  training.cache()
  validation.cache()
  test.cache()

  logger.info(s"training data count: ${training.count()}")

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
      unpersistModel(model)
      (score, model, params, buildTime)
    }
  }.sortBy(-_._1).toList

  val best = results.head
  val (score, model, (rank, lambda, numIter), _) = best

  dumpScores(results, directory + "/scores.txt")

  logger.info(s"The best model params: rank=$rank, lambda=$lambda, numIter=$numIter. It's ROC AUC = $score")

  val testSetScore = Evaluations.evaluateAndGiveAUC(new ALSPrediction(model), test, directory + "/best-on-test-set")
  logger.info(s"ROC AUC of best model on test set = $testSetScore")

  val noPersonalizationPredictor = new NoPersonalizationPrediction(training, defaultRank = 2.5)
  val baselineScore = Evaluations.evaluateAndGiveAUC(noPersonalizationPredictor, validation, directory + "/baseline-product-mean")
  logger.info(s"baseline ROC AUC (no personalization) = $baselineScore")

  val randomPredictor = new RandomPrediction(training)
  val randomScore = Evaluations.evaluateAndGiveAUC(randomPredictor, validation, directory + "/baseline-random")
  logger.info(s"baseline ROC AUC (random) = $randomScore")

  sparkCassandraClient.saveModel(model)
  logger.info(s"The best model params: rank=$rank, lambda=$lambda, numIter=$numIter. It's ROC AUC = $score")

  sparkCassandraClient.sparkContext.stop()

  private def unpersistModel(model: MatrixFactorizationModel): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }
  
  private def dumpScores(scores: List[(Double, MatrixFactorizationModel, (Int, Double, Int), Double)], filename: String) = {
    val header = "rank,lambda,numIter,score,time\n"
    val csv = scores.map {
      case (score, _, params, buildTime) => params.productIterator.toList.mkString(",") + "," + score.toString + "," + buildTime.toString
    }.mkString("\n")

    File(filename).writeAll(header, csv)
  }

  private def time[A](f: => A): (A, Double) = {
    val start = System.nanoTime
    val result = f
    val elapsedTimeInMs = (System.nanoTime - start) / 1e6
    (result, elapsedTimeInMs)
  }
}
