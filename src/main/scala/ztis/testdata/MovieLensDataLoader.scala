package ztis.testdata

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.{SparkConf, SparkContext}
import ztis.{UserOrigin, ExplicitAssociationEntry}

import sys.process._
import java.net.URL
import java.io.File

object MovieLensDataLoader extends App with StrictLogging {
  try {

    val config = ConfigFactory.load("testdata")
    // downloadDataset(config)
    val spark = setupSpark()
    putDataToDatastore(spark)
    spark.stop()

  } catch {
    case e: Exception => logger.error("Error during loading test data", e)
  }

  private def putDataToDatastore(spark: SparkContext): Unit = {
    val ratingFile = spark.textFile("ml-1m/ratings.dat")
    val ratings = ratingFile.map(toAssociationEntry)
    val onlyPositiveRatings = ratings.filter(_.rating > 0)

    logger.info("Count: " + onlyPositiveRatings.count())
  }

  private def toAssociationEntry(line: String) = {
    val fields = line.split("::")

    val rating01 = fields(2).toInt match {
      case x if x >= 4 => 1
      case _ => 0
    }

    ExplicitAssociationEntry(
      userName = fields(0),
      origin = UserOrigin.Twitter,
      link = fields(1),
      rating = rating01)
  }

  private def setupSpark(): SparkContext = {
    val conf = new SparkConf()
      .setMaster(s"local[2]")
      .setAppName("MovieLensLoader")
      .set("spark.executor.memory", "8g")

    new SparkContext(conf)
  }

  private def downloadDataset(config: Config): Unit = {
    val datasetUrl = config.getString("testdata.url")

    logger.info("Downloading a dataset")
    new URL(datasetUrl) #> new File("dataset.zip") !!

    logger.info("Dataset downloaded")

    "unzip dataset.zip" !!

    logger.info("Dataset unzipped")
  }
}
