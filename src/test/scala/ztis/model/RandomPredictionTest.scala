package ztis.model

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext._
import org.scalactic.Tolerance._

class RandomPredictionTest extends SparkSpec {
  "Prediction" should "return random rating from all ratings distribution" in {
    val trainingData = sc.parallelize(Array(
      Rating(user=1, product=1, rating=1.0),
      Rating(user=2, product=1, rating=2.0),
      Rating(user=1, product=2, rating=2.0),
      Rating(user=2, product=2, rating=3.0),
      Rating(user=1, product=3, rating=3.0),
      Rating(user=2, product=3, rating=3.0),
      Rating(user=1, product=4, rating=4.0),
      Rating(user=2, product=4, rating=4.0),
      Rating(user=1, product=5, rating=4.0),
      Rating(user=2, product=5, rating=4.0)
    ))

    // count ratio 1.0 : 2.0 : 3.0 : 4.0 == 1 : 2 : 3 : 4

    val predictor = new RandomPrediction(trainingData)

    val all = 10000
    val userProducts = sc.parallelize((1 to all).map(i => (i, i)))

    val predictionPercentages = predictor.predictMany(userProducts).groupBy(_.rating).mapValues(_.size / all.toDouble).collect().toMap

    assert(predictionPercentages(1.0) === 0.10 +- 0.01)
    assert(predictionPercentages(2.0) === 0.20 +- 0.01)
    assert(predictionPercentages(3.0) === 0.30 +- 0.01)
    assert(predictionPercentages(4.0) === 0.40 +- 0.01)
  }
}
