package ztis.model

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import ztis.Spark
import org.apache.spark.SparkContext._
import ztis.cassandra.{CassandraConfiguration, CassandraClient, SparkCassandraClient}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.reflect.io.File
import scala.sys.process._

object ModelBuilder extends App with StrictLogging {
  val config = ConfigFactory.load("model")
  val cassandraConfig = CassandraConfiguration(config)
  val sparkConfig = SparkCassandraClient.setCassandraConfig(Spark.baseConfiguration("ModelBuilder"), cassandraConfig)
  val sparkCassandraClient = new SparkCassandraClient(new CassandraClient(cassandraConfig), Spark.sparkContext(sparkConfig))
  val ratings = sparkCassandraClient.ratingsRDD

  val Array(training, maybeValidation, maybeTest) = ratings.randomSplit(Array(0.6, 0.2, 0.2))
  
  val complementValidationSets = config.getBoolean("model.complement-validation-sets")
  val complementFactor = config.getDouble("model.complement-factor")

  val (validation, test) = if(complementValidationSets) {
    (complementDataset(maybeValidation, complementFactor), complementDataset(maybeTest, complementFactor))
  }
  else {
    (maybeValidation, maybeTest)
  }
  
  training.cache()
  validation.cache()
  test.cache()
  test.checkpoint()

  val ranks = config.getIntList("model.params.ranks").asScala.toVector.map(_.toInt)
  val lambdas = config.getDoubleList("model.params.lambdas").asScala.toVector.map(_.toDouble)
  val numIters = config.getIntList("model.params.iterations").asScala.toVector.map(_.toInt)
  val alphas = config.getDoubleList("model.params.alphas").asScala.toVector.map(_.toDouble)
  
  val directory = s"report-${DateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd--HH-mm-ss"))}"
    s"mkdir $directory" !!
  
  val minRatingToConsiderAsGood = config.getInt("model.min-rating-to-consider-as-good")
  val alsModelType = config.getString("model.als-model-type")
  
  val paramsCrossProduct = for {
    rank <- ranks; lambda <- lambdas; numIter <- numIters; alpha <- alphas
  } yield (rank, lambda, numIter, alpha)

  type Result = (Double, MatrixFactorizationModel, (Int, Double, Int, Double), Double)

  var best : (Double, MatrixFactorizationModel, (Int, Double, Int, Double), Double) = null
  val results = ArrayBuffer[Result]()

  paramsCrossProduct.foreach {
    case params @ (rank, lambda, numIter, alpha) => {
      val modelDirectory = s"$directory/r$rank-l${(lambda * 100).toInt}-i$numIter-a${(alpha * 100).toInt}"
      val (model, buildTime) = time {
        if (alsModelType == "explicit") {
          ALS.train(training, rank, numIter, lambda)
        }
        else {
          ALS.trainImplicit(training, rank, numIter, lambda, alpha)
        }
      }
      val score = Evaluations.evaluateAndGiveAUC(new ALSPrediction(model), validation,
        minRatingToConsiderAsGood, modelDirectory, buildTime)
      unpersistModel(model)

      if (best == null || best._1 < score) {
        best = (score, model, params, buildTime)
      }

      results.append((score, null, params, buildTime))
    }
  }
  
  val (score, model, (rank, lambda, numIter, alpha), _) = best

  dumpScores(results.toVector, directory + "/scores.txt")
  logger.info(s"The best model params: rank=$rank, lambda=$lambda, numIter=$numIter, alpha=$alpha. It's ROC AUC = $score")
  
  val testSetScore =  Evaluations.evaluateAndGiveAUC(new ALSPrediction(model), test,
    minRatingToConsiderAsGood, directory + "/best-on-test-set")
  logger.info(s"ROC AUC of best model on test set = $testSetScore")

  val noPersonalizationPredictor = new NoPersonalizationPrediction(training,
    defaultRank = config.getInt("model.no-personalization-default-rank"))
  val baselineScore = Evaluations.evaluateAndGiveAUC(noPersonalizationPredictor, validation,
    minRatingToConsiderAsGood, directory + "/baseline-product-mean")
  logger.info(s"baseline ROC AUC (no personalization) = $baselineScore")

  val randomPredictor = new RandomPrediction(training)
  val randomScore = Evaluations.evaluateAndGiveAUC(randomPredictor, validation,
    minRatingToConsiderAsGood, directory + "/baseline-random")
  logger.info(s"baseline ROC AUC (random) = $randomScore")

  sparkCassandraClient.saveModel(model)
  sparkCassandraClient.sparkContext.stop()
  
  private def unpersistModel(model: MatrixFactorizationModel): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

  private def dumpScores(scores : Vector[(Double, MatrixFactorizationModel, (Int, Double, Int, Double), Double)], filename: String) = {
    val header = "rank,lambda,numIter,alpha,score,time\n"
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

  private def complementDataset(positiveDataset: RDD[Rating], complementFactor : Double) = {
    val users = positiveDataset.map {
      case Rating(user, _, _) => user
    }.distinct()

    val products = positiveDataset.map {
      case Rating(_, product, _) => product
    }.distinct()

    val ratingsNumber = (positiveDataset.count() * (complementFactor - 1.0 )).round.toInt

    val positiveUserProduct = positiveDataset.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }

    val fraction = ratingsNumber.toDouble / users.count()
    val moreUsers = users.sample(true, fraction).zipWithIndex.map {case (x, y) => (y, x)}
    val moreProducts = products.sample(true, fraction).zipWithIndex.map {case (x, y) => (y, x)}

    val additional = moreUsers.join(moreProducts).map {
      case (_, (user, product)) => ((user, product), 0.0)
    }

    additional.fullOuterJoin(positiveUserProduct).map {
      case ((user, product), (firstRating, secondRating)) => {
        val rating = firstRating.getOrElse(0.0) + secondRating.getOrElse(0.0)
        Rating(user, product, rating)
      }
    }
  }
}
