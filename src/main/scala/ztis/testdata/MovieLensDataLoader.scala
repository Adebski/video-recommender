package ztis.testdata

import java.io.File
import java.net.URL

import com.datastax.spark.connector._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.{SparkConf, SparkContext}
import ztis.CassandraClient

import scala.collection.JavaConverters._
import scala.sys.process._

object MovieLensDataLoader extends App with StrictLogging {

  try {

    val config = ConfigFactory.load("testdata")
    bootstrapCassandra(config)
    // downloadDataset(config)
    val spark = setupSpark(config)
    putDataToDatastore(spark, config)
    spark.stop()

  } catch {
    case e: Exception => logger.error("Error during loading test data", e)
  }

  private def bootstrapCassandra(config: Config) = {
    val cassandraClient = new CassandraClient(config)
  }

  private def putDataToDatastore(spark: SparkContext, config: Config): Unit = {
    val keyspace = config.getString("cassandra.keyspace")
    val explicitAssocTableName = config.getString("cassandra.explicit-association-table-name")

    val ratingFile = spark.textFile("ml-1m/ratings.dat")
    val ratings = ratingFile.map(toAssociationEntry)
    val onlyPositiveRatings = ratings.filter(_._4 > 0)

    onlyPositiveRatings.saveToCassandra(keyspace, explicitAssocTableName.toLowerCase,
                                        SomeColumns("user_id", "user_origin", "link", "rating"))
  }

  private def toAssociationEntry(line: String) = {
    val fields = line.split("::")

    val rating01 = fields(2).toInt match {
      case x if x >= 4 => 1
      case _ => 0
    }

    (fields(0), "movielens", fields(1), rating01)
  }

  private def setupSpark(config: Config): SparkContext = {
    val cassandraHost = config.getStringList("cassandra.contact-points").asScala.iterator.next().split(":")(0)

    val sparkConfig = new SparkConf()
      .setMaster(s"local[2]")
      .setAppName("MovieLensLoader")
      .set("spark.executor.memory", "8g")
      .set("spark.cassandra.connection.host", cassandraHost)

    new SparkContext(sparkConfig)
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
