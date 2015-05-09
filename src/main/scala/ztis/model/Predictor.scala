package ztis.model

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

trait Predictor {
  def predictMany(userProducts: RDD[(Int, Int)]) : RDD[Rating]
  def unpersist(): Unit = {}
}
