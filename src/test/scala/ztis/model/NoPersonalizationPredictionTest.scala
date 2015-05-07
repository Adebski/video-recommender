package ztis.model

import org.apache.spark.mllib.recommendation.Rating
import org.scalactic.TolerantNumerics

class NoPersonalizationPredictionTest extends SparkSpec {
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(0.001)

  "Prediction" should "return default rank when no data is available" in {
    val defaultRank = 2.5
    val trainingData = sc.parallelize(Array[Rating]())

    val predictor = new NoPersonalizationPrediction(trainingData, defaultRank)

    val user1 = 1
    val user2 = 2
    val product1 = 1
    val product2 = 2

    val userProducts = sc.parallelize(Array((user1, product1), (user2, product2)))

    val predictions: Array[Rating] = predictor.predictMany(userProducts).collect()
    val predictionsMap: Map[(Int, Int), Double] = predictions.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }.toMap

    assert(predictionsMap.size == 2)

    assert(predictionsMap((user1, product1)) === defaultRank)
    assert(predictionsMap((user2, product2)) === defaultRank)
  }

  it should "return average rating of a product" in {
    val defaultRank = 2.5
    val trainingData = sc.parallelize(Array(
      Rating(user=1, product=1, rating=4.0),
      Rating(user=2, product=1, rating=5.0),
      Rating(user=1, product=2, rating=1.0),
      Rating(user=2, product=2, rating=2.0)
    ))

    val predictor = new NoPersonalizationPrediction(trainingData, defaultRank)

    val userProducts = sc.parallelize(Array((3, 1), (3, 2)))

    val predictions: Array[Rating] = predictor.predictMany(userProducts).collect()
    val predictionsMap: Map[(Int, Int), Double] = predictions.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }.toMap

    assert(predictionsMap((3, 1)) === 4.5)
    assert(predictionsMap((3, 2)) === 1.5)
  }

  it should "make product ratings proportional to the number of raters" in {
    val defaultRank = 3.0
    val trainingData = sc.parallelize(Array(
      Rating(user=1, product=1, rating=4.0),
      Rating(user=2, product=1, rating=4.0),
      Rating(user=2, product=2, rating=4.0),
      Rating(user=3, product=1, rating=4.0),
      Rating(user=3, product=2, rating=4.0),
      Rating(user=3, product=3, rating=4.0)
    ))

    val predictor = new NoPersonalizationPrediction(trainingData, defaultRank)

    val userProducts = sc.parallelize(Array((4, 1), (4, 2), (4, 3)))

    val predictions: Array[Rating] = predictor.predictMany(userProducts).collect()
    val predictionsMap: Map[(Int, Int), Double] = predictions.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }.toMap

    val firstProductRating = predictionsMap((4, 1))
    val secondProductRating = predictionsMap((4, 2))
    val thirdProductRating = predictionsMap((4, 3))

    assert(firstProductRating > secondProductRating)
    assert(secondProductRating > thirdProductRating)
  }

}
