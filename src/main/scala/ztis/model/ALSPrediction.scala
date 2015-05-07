package ztis.model

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

class ALSPrediction(model: MatrixFactorizationModel) extends Predictor {
  override def predictMany(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    model.predict(userProducts)
  }

  override def unpersist(): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }
}
