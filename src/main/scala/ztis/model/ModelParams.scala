package ztis.model

import com.typesafe.config.Config
import scala.collection.JavaConverters._

case class ModelParams(rank: Int, lambda: Double, numIter: Int, alpha: Double, followerFactor: Double) {
  def toCsv : String = s"$rank,$lambda,$numIter,$alpha,$followerFactor"
  def toDirName : String = s"r$rank-l${(lambda * 100).toInt}-i$numIter-a${(alpha * 100).toInt}-ff${(followerFactor * 100).toInt}"
  override def toString : String = s"rank=$rank, lambda=$lambda, numIter=$numIter, alpha=$alpha, followerFactor=$followerFactor"
}

object ModelParams {
  def paramsCrossProduct(config: Config) : Vector[ModelParams] = {
    val ranks = config.getIntList("ranks").asScala.toVector.map(_.toInt)
    val lambdas = config.getDoubleList("lambdas").asScala.toVector.map(_.toDouble)
    val numIters = config.getIntList("iterations").asScala.toVector.map(_.toInt)
    val alphas = config.getDoubleList("alphas").asScala.toVector.map(_.toDouble)
    val followerFactors = config.getDoubleList("follower-factors").asScala.toVector.map(_.toDouble)

    for {
      rank <- ranks; lambda <- lambdas; numIter <- numIters; alpha <- alphas; followerFactor <- followerFactors
    } yield ModelParams(rank, lambda, numIter, alpha, followerFactor)
  }

  def csvHeader : String = "rank,lambda,numIter,alpha,followerFactor"
}
