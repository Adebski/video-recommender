package ztis.model

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class NoPersonalizationPrediction(trainingData: RDD[Rating], defaultRank: Double) extends Predictor with Serializable {
  private val productMeanRanks = computeMeans()
  productMeanRanks.cache()

  def predictMany(userProducts: RDD[(Int, Int)]) : RDD[Rating] = {
    val productUsers = userProducts.map {
      case (user, product) => (product, user)
    }

    productUsers.leftOuterJoin(productMeanRanks).map {
      case (product, (user, maybeRating)) => Rating(user, product, maybeRating.getOrElse(defaultRank))
    }
  }

  override def unpersist() = {
    productMeanRanks.unpersist()
  }

  private def computeMeans(): RDD[(Int, Double)] = {
    val allUsers = trainingData.map(_.user).distinct().count()

    def meanProductRank(ranks: Iterable[Rating]) = {
      val seq = ranks.toSeq
      val noRatingUsers = allUsers - seq.length

      (seq.map(_.rating).sum + (noRatingUsers * defaultRank) ) / allUsers.toDouble
    }

    trainingData.groupBy(_.product).mapValues(meanProductRank)
  }

}
