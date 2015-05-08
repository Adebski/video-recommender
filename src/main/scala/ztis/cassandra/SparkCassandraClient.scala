package ztis.cassandra

import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import ztis.UserAndRating

class SparkCassandraClient(val client: CassandraClient, val sparkContext: SparkContext) extends StrictLogging {
  
  private val columns = SomeColumns("user_id" , "user_origin", "link", "rating", "timesUpvotedByFriends")
  
  def ratingsRDD: RDD[CassandraRow] = {
    sparkContext.cassandraTable(client.keyspace, client.ratingsTableName)
  }
  
  def saveRatings(rdd: RDD[UserAndRating]): RDD[UserAndRating] = {
    rdd.map(_.toTuple).saveToCassandra(client.keyspace, client.ratingsTableName, columns)
    rdd
  }
  
}

object SparkCassandraClient {
  def setCassandraConfig(sparkConfig: SparkConf, appConfig: Config): SparkConf = {
    val contactPoint = CassandraClient.contactPoints(appConfig).get(0)

    sparkConfig.set("spark.cassandra.connection.host", contactPoint.getHostString)
    sparkConfig.set("spark.cassandra.connection.native.port", contactPoint.getPort.toString)
  
    sparkConfig
  }
}
