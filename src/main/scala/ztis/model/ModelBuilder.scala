package ztis.model

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating, ALS}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.evaluation.{RegressionMetrics, BinaryClassificationMetrics}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.reflect.io.File
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

  logger.info(s"Count: ${training.count()}\n")

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
        s"mkdir $modelDirectory" !!

        val model = ALS.train(training, rank, numIter, lambda)
        val score = evaluate(model, modelDirectory)

        (score, model, params)
      }
    }.sortBy(-_._1).toList

    val best = results.head
    val (score, model, (rank, lambda, numIter)) = best

    dumpScores(results, directory + "/scores.txt")

    persistInDatabase(model)
    logger.info(s"The best model params: rank=$rank, lambda=$lambda, numIter=$numIter. It's ROC AUC = $score")

    s"mkdir ${directory + "/baseline-product-mean"}" !!

    compareWithNoPersonalizationPrediction(directory + "/baseline-product-mean")
  }

  spark.stop()

  def dumpScores(scores : List[(Double, MatrixFactorizationModel, (Int, Double, Int))], filename: String) = {
    val header = "rank,lambda,numIter,score\n"
    val csv = scores.map {
      case (score, _, params) => params.productIterator.toList.mkString(",") + "," + score.toString
    }.mkString("\n")

    File(filename).writeAll(header, csv)
  }

  def evaluate(model: MatrixFactorizationModel, reportDirectory: String) = {
    val userProducts = validation.map(rating => (rating.user, rating.product))
    val predictions = model.predict(userProducts)
    val keyedPredictions = predictions.map(rating => ((rating.user, rating.product), rating.rating))
    val keyedObservations = validation.map(rating => ((rating.user, rating.product), rating.rating))
    val predictionAndObservations = keyedPredictions.join(keyedObservations).values

    val regressionMetrics = new RegressionMetrics(predictionAndObservations)
    val rmse = regressionMetrics.rootMeanSquaredError

    def isGoodEnough(observation: Double) = if (observation > 3) 1.0 else 0.0

    val scoresAndLabels = predictionAndObservations.map {
      case (prediction, observation) => (prediction, isGoodEnough(observation))
    }

    scoresAndLabels.cache()
    val fraction = 1000.0 / scoresAndLabels.count()

    val metrics = new BinaryClassificationMetrics(scoresAndLabels)
    val prArea = metrics.areaUnderPR()
    val rocArea = metrics.areaUnderROC()

    shortRddToFile(reportDirectory + "/pr.dat",
      metrics.pr().map {
        case (recall, precision) => s"$recall, $precision"
      }.sample(false, fraction)
    )
    shortRddToFile(reportDirectory + "/roc.dat",
      metrics.roc().map {
        case (rate1, rate2) => s"$rate1, $rate2"
      }.sample(false, fraction)
    )

    shortRddToFile(reportDirectory + "/thresholds.dat",
      metrics.precisionByThreshold().join(metrics.recallByThreshold()).join(metrics.fMeasureByThreshold()).map {
        case (threshold, ((precision, recall), fMeasure)) => s"$threshold, $precision, $recall, $fMeasure"
      }.sample(false, fraction)
    )

    val report =
      s"""|
          |RMSE: $rmse
          |AUC PR: $prArea
          |AUC ROC: $rocArea
          |""".stripMargin

    logger.info(report)
    File(reportDirectory + "/report.txt").writeAll(report)

    s"./plots.sh $reportDirectory" !!

    rocArea
  }

  def compareWithNoPersonalizationPrediction(reportDirectory: String) = {
    val predictor = new NoPersonalizationPrediction(training, defaultRank = 2.5)

    val userProducts = validation.map(rating => (rating.user, rating.product))
    val predictions = predictor.predictMany(userProducts)

    val keyedPredictions = predictions.map(rating => ((rating.user, rating.product), rating.rating))
    val keyedObservations = validation.map(rating => ((rating.user, rating.product), rating.rating))
    val predictionAndObservations = keyedPredictions.join(keyedObservations).values

    val regressionMetrics = new RegressionMetrics(predictionAndObservations)
    val rmse = regressionMetrics.rootMeanSquaredError

    def isGoodEnough(observation: Double) = if (observation > 3) 1.0 else 0.0

    val scoresAndLabels = predictionAndObservations.map {
      case (prediction, observation) => (prediction, isGoodEnough(observation))
    }

    val metrics = new BinaryClassificationMetrics(scoresAndLabels)
    val prArea = metrics.areaUnderPR()
    val rocArea = metrics.areaUnderROC()

    shortRddToFile(reportDirectory + "/pr.dat",
      sampleAtMost(1000, metrics.pr().map {
        case (recall, precision) => s"$recall, $precision"
      })
    )
    shortRddToFile(reportDirectory + "/roc.dat",
      sampleAtMost(1000, metrics.roc().map {
        case (rate1, rate2) => s"$rate1, $rate2"
      })
    )

    shortRddToFile(reportDirectory + "/thresholds.dat",
      sampleAtMost(1000,
        metrics.precisionByThreshold().join(metrics.recallByThreshold()).join(metrics.fMeasureByThreshold()).map {
          case (threshold, ((precision, recall), fMeasure)) => s"$threshold, $precision, $recall, $fMeasure"
        }
      )
    )

    val report =
      s"""|
         |RMSE: $rmse
          |AUC PR: $prArea
          |AUC ROC: $rocArea
          |""".stripMargin

    logger.info(report)
    File(reportDirectory + "/report.txt").writeAll(report)

    s"./plots.sh $reportDirectory" !!

    rocArea
  }

  private def sampleAtMost(elements: Int, rdd: RDD[String]) = {
    val count = rdd.count()
    if(count <= elements) {
      rdd
    }
    else {
      rdd.sample(false, elements / count.toDouble)
    }
  }

  private def shortRddToFile(filename: String, rdd: RDD[String]) = {
    File(filename).writeAll(rdd.collect().mkString("\n"))
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

}
