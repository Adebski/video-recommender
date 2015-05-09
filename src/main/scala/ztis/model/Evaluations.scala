package ztis.model

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, RegressionMetrics}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import scala.sys.process._
import org.apache.spark.SparkContext._

import scala.reflect.io.File
import scala.language.postfixOps

object Evaluations extends StrictLogging {

  def evaluateAndGiveAUC(predictor: Predictor, validationData: RDD[Rating], reportDirectory: String, buildTime: Double = -1.0) = {
    s"mkdir $reportDirectory" !!

    val userProducts = validationData.map(rating => (rating.user, rating.product))
    val predictions = predictor.predictMany(userProducts)

    val keyedPredictions = predictions.map(rating => ((rating.user, rating.product), rating.rating))
    val keyedObservations = validationData.map(rating => ((rating.user, rating.product), rating.rating))
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
          |Build time: $buildTime
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
}
