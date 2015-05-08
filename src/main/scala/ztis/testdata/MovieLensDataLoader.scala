package ztis.testdata

import java.io.File
import java.net.URL

import com.datastax.spark.connector._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.{SparkConf, SparkContext}
import ztis.cassandra.CassandraClient

import scala.language.postfixOps
import scala.collection.JavaConverters._
import scala.sys.process._

object MovieLensDataLoader extends App with StrictLogging {

  try {
    val config = ConfigFactory.load("testdata")
    bootstrapCassandra(config)
    downloadDataset(config)

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
    val unaryScale = config.getBoolean("testdata.unary-scale")

    val keyspace = config.getString("cassandra.keyspace")
    val explicitAssocTableName = config.getString("cassandra.ratings-table-name")

    val ratingFile = spark.textFile("ml-1m/ratings.dat")
    val ratings = if(unaryScale) {
      ratingFile.map(toAssociationEntry).filter(_._4 > 3).map(_.copy(_4 = 1))
    }
    else {
      ratingFile.map(toAssociationEntry)
    }

    ratings.saveToCassandra(keyspace, explicitAssocTableName.toLowerCase,
                            SomeColumns("user_id", "user_origin", "link", "rating"))
  }

  private def toAssociationEntry(line: String) = {
    val fields = line.split("::")
    (fields(0), "movielens", fields(1), fields(2).toInt)
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
    val filename = "dataset.zip"

    if(new File(filename).exists()) {
      logger.info(s"File $filename already exists. Skipping downloading...")
      return
    }

    val datasetUrl = config.getString("testdata.url")

    logger.info("Downloading a dataset")
    new URL(datasetUrl) #> new File(filename) !!

    logger.info("Dataset downloaded")

    s"unzip $filename" !!

    logger.info("Dataset unzipped")
  }
}
