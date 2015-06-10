package ztis.model

import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import ztis.model.ModelBuilderInitializer.Result
import ztis.{Initializer, UserVideoFullInformation, Spark}
import org.apache.spark.SparkContext._
import ztis.cassandra.{CassandraConfiguration, CassandraClient, SparkCassandraClient}

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.reflect.io.File
import scala.sys.process._

object ModelBuilderInitializer {
  case class Result(score: Double, model: MatrixFactorizationModel, params: ModelParams, time: Double)

  def toRating(userVideoFullInformation : UserVideoFullInformation) : Rating = {
    Rating(userVideoFullInformation.userID, userVideoFullInformation.videoID, userVideoFullInformation.rating)
  }

  def toConfidence(userVideoFullInformation: UserVideoFullInformation, followersFactor: Double): Rating = {
    val confidence = userVideoFullInformation.rating + followersFactor * userVideoFullInformation.timesRatedByFollowedUsers
    Rating(userVideoFullInformation.userID, userVideoFullInformation.videoID, confidence)
  }
}

class ModelBuilderInitializer extends Initializer {
  import ModelBuilderInitializer._

  override def initialize(): Unit = {
    val config = ConfigFactory.load("model")
    val cassandraConfig = CassandraConfiguration(config)
    val sparkConfig = SparkCassandraClient.setCassandraConfig(Spark.baseConfiguration("ModelBuilder"), cassandraConfig)
    val sparkCassandraClient = new SparkCassandraClient(new CassandraClient(cassandraConfig), Spark.sparkContext(sparkConfig))
    val ratings = sparkCassandraClient.userVideoFullInformation

    val Array(fullTraining, fullValidation, fullTest) = ratings.randomSplit(Array(0.6, 0.2, 0.2))

    val complementValidationSets = config.getBoolean("model.complement-validation-sets")
    val complementFactor = config.getDouble("model.complement-factor")

    val maybeValidation = fullValidation.map(toRating)
    val maybeTest = fullTest.map(toRating)

    val (validation, test) = if(complementValidationSets) {
      (complementDataset(maybeValidation, complementFactor), complementDataset(maybeTest, complementFactor))
    }
    else {
      (maybeValidation, maybeTest)
    }

    val training = fullTraining.map(toRating)

    training.cache()
    validation.cache()
    test.cache()

    val directory = s"report-${DateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd--HH-mm-ss"))}"
    s"mkdir $directory" !!

    val minRatingToConsiderAsGood = config.getInt("model.min-rating-to-consider-as-good")
    val alsModelType = config.getString("model.als-model-type")

    var best : Result = null
    val results = ArrayBuffer[Result]()

    ModelParams.paramsCrossProduct(config.getConfig("model.params")).foreach {
      case params : ModelParams => {
        val modelDirectory = s"$directory/" + params.toDirName

        val (model, buildTime) = time {
          if (alsModelType == "explicit") {
            ALS.train(training, params.rank, params.numIter, params.lambda)
          }
          else {
            val confidenceTraining = fullTraining.map(preference => toConfidence(preference, params.followerFactor))
            ALS.trainImplicit(confidenceTraining, params.rank, params.numIter, params.lambda, params.alpha)
          }
        }
        val score = Evaluations.evaluateAndGiveAUC(new ALSPrediction(model), validation,
          minRatingToConsiderAsGood, modelDirectory, buildTime)
        unpersistModel(model)

        if (best == null || best.score < score) {
          best = Result(score, model, params, buildTime)
        }

        results.append(Result(score, null, params, buildTime))
      }
    }

    val Result(score, model, params, _) = best

    dumpScores(results.toVector, directory + "/scores.txt")
    logger.info(s"The best model params: ${params.toString}. It's ROC AUC = $score")

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
    sparkCassandraClient.stop()
  }

  private def unpersistModel(model: MatrixFactorizationModel): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

  private def dumpScores(scores : Vector[Result], filename: String) = {
    val header = ModelParams.csvHeader + ",score,time\n"
    val csv = scores.map {
      case Result(score, _, params, buildTime) => params.toCsv + "," + score.toString + "," + buildTime.toString
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
