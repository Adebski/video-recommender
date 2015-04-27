package ztis.model

import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

import scala.collection.JavaConverters._

object ModelValidator extends App with StrictLogging {
  val config = ConfigFactory.load("testdata")
  val spark = setupSpark(config)

  val associations = associationRdd.sample(withReplacement = false, fraction = 0.001)

  val Array(training, validation, test) = associations.randomSplit(Array(0.6, 0.2, 0.2))

  println(s"training: ${training.count()}, validation: ${validation.count()}, test: ${test.count()}")

  spark.stop()

  // extract to CassandraClient or sth like this
  private def associationRdd = {
    val keyspace = config.getString("cassandra.keyspace")
    val explicitAssocTableName = config.getString("cassandra.explicit-association-table-name")

    spark.cassandraTable(keyspace, explicitAssocTableName.toLowerCase)
  }





  // TODO: copied from MovieLensDataLoder, adhere to DRY,
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
