package ztis.model

import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating, ALS}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.SparkContext._

import scala.collection.JavaConverters._

object ModelBuilder extends App with StrictLogging {
  val config = ConfigFactory.load("testdata")
  val spark = setupSpark(config)

  val ratings = associationRdd.map(row => {
    // temporarily
    val userId = row.getString("user_id").toInt
    val link = row.getString("link").toInt
    val rating = row.getInt("rating")

    Rating(userId, link, rating.toDouble)
  })

  // will change this in the next PR
  val training = ratings

  training.cache()
  logger.info(s"Count: ${training.count()}\n")

  val rank = 8
  val lambda = 0.1
  val numIters = 10

  val model: MatrixFactorizationModel = ALS.train(training, rank, numIters, lambda)


  spark.stop()

  // TODO(#8): extract to CassandraClient or sth like this
  private def associationRdd = {
    val keyspace = config.getString("cassandra.keyspace")
    val explicitAssocTableName = config.getString("cassandra.explicit-association-table-name")

    spark.cassandraTable(keyspace, explicitAssocTableName.toLowerCase)
  }

  // TODO(#8): copied from MovieLensDataLoader, adhere to DRY,
  private def setupSpark(config: Config): SparkContext = {
    val cassandraHost = config.getStringList("cassandra.contact-points").asScala.iterator.next().split(":")(0)

    val sparkConfig = new SparkConf()
      .setMaster(s"local[2]")
      .setAppName("ModelValidator")
      .set("spark.executor.memory", "8g")
      .set("spark.cassandra.connection.host", cassandraHost)

    new SparkContext(sparkConfig)
  }

}
