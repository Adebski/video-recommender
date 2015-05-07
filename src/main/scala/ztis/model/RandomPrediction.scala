package ztis.model

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import scala.util.Random

class RandomPrediction(trainingData: RDD[Rating]) extends Predictor with Serializable {
  private val ratingsDistribution: Array[(Double, Int)] = trainingData.groupBy(_.rating).mapValues(_.size).collect()
  private val count = ratingsDistribution.map(_._2).sum

  def randomRating(): Double = {
    val i = Random.nextInt(count)
    val (rating, _) = ratingsDistribution.reduceLeft[(Double, Int)] {
      case ((rating1, count1), (rating2, count2)) => {
        if(i < count1) {
          (rating1, count1)
        } else {
          (rating2, count1 + count2)
        }
      }
    }

    rating
  }

  override def predictMany(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    userProducts.map {
      case (user, product) => Rating(user, product, randomRating())
    }
  }
}
