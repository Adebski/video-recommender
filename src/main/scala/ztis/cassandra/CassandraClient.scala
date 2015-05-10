package ztis.cassandra

import java.net.InetSocketAddress
import java.util

import com.datastax.driver.core.policies.RoundRobinPolicy
import com.datastax.driver.core.{Cluster, ResultSet}
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.UserAndRating

import scala.collection.JavaConverters._

class CassandraClient(config: Config) extends StrictLogging {

  private[cassandra] val keyspace: String = CassandraClient.keyspace(config)

  private[cassandra] val ratingsTableName = CassandraClient.ratingsTableName(config)

  private[cassandra] val userFeaturesTableName = CassandraClient.userFeaturesTableName(config)
  
  private[cassandra] val productFeaturesTableName = CassandraClient.productFeaturesTableName(config)
  
  private[cassandra] val contactPoints = CassandraClient.contactPoints(config)
  
  private val cluster = Cluster.builder().addContactPointsWithPorts(contactPoints)
    .withLoadBalancingPolicy(new RoundRobinPolicy)
    .build()
  private val session = cluster.connect()

  session.execute(CassandraClient.createKeyspaceQuery(keyspace))
  session.execute(CassandraClient.createRatingsTableQuery(keyspace, ratingsTableName))
  val preparedInsertToRatings = session.prepare(CassandraClient.insertToRatingsQuery(keyspace, ratingsTableName))

  def updateRating(userAndRating: UserAndRating): Unit = {
    val rating: java.lang.Integer = userAndRating.rating
    val timesUpvotedByFriends: java.lang.Integer = userAndRating.timesUpvotedByFriends
    
    val statement = preparedInsertToRatings.bind(userAndRating.userName,
      userAndRating.origin.name, 
      userAndRating.link,
      rating,
      timesUpvotedByFriends)
    session.execute(statement)
  }

  def clean(): Unit = {
    logger.info(s"Dropping keyspace $keyspace")
    session.execute(CassandraClient.dropKeyspaceQuery(keyspace))
  }

  def dropTable(keyspace: String, table: String): Unit = {
    logger.info(s"Dropping table $keyspace.$table")
    session.execute(CassandraClient.dropTableQuery(keyspace, table))
  }
  
  def allRatings: ResultSet = {
    session.execute(CassandraClient.selectAll(keyspace, ratingsTableName))
  }
}

object CassandraClient {

  def keyspace(config: Config): String = {
    config.getString("cassandra.keyspace")  
  }
  
  def ratingsTableName(config: Config): String = {
    config.getString("cassandra.ratings-table-name")  
  }
  
  def userFeaturesTableName(config: Config): String = {
    config.getString("cassandra.user-features-table-name")
  }
  
  def productFeaturesTableName(config: Config): String = {
    config.getString("cassandra.product-features-table-name")  
  }
  
  def contactPoints(config: Config): util.List[InetSocketAddress] = {
    config.getStringList("cassandra.contact-points").asScala.map(addressToInetAddress).asJava
  }

  private def addressToInetAddress(address: String): InetSocketAddress = {
    val hostAndPort = address.split(":")

    new InetSocketAddress(hostAndPort(0), hostAndPort(1).toInt)
  }
  
  def createKeyspaceQuery(keyspace: String): String =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS "$keyspace" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """.stripMargin

  def dropKeyspaceQuery(keyspace: String): String = {
    s"""DROP KEYSPACE "$keyspace" """
  }

  def dropTableQuery(keyspace: String, table: String): String = {
    s"""DROP TABLE IF EXISTS "$keyspace"."$table" """
  }
  
  def createRatingsTableQuery(keyspace: String, tableName: String): String =
    s"""
       |CREATE TABLE IF NOT EXISTS "$keyspace"."$tableName" (
                                                         |"user_id" text,
                                                         |"user_origin" text,
                                                         |"link" text,
                                                         |"rating" int,
                                                         |"timesUpvotedByFriends" int,
                                                         |PRIMARY KEY(("user_id", "user_origin"), "link"))
     """.stripMargin

  def insertToRatingsQuery(keyspace: String, tableName: String): String =
    s"""
       |INSERT INTO "$keyspace"."$tableName" ("user_id", "user_origin", "link", "rating", "timesUpvotedByFriends") VALUES (?, ?, ?, ?, ?)
     """.stripMargin

  def selectAll(keyspace: String, tableName: String): String = {
    s"""SELECT * FROM "$keyspace"."$tableName" """
  }
}