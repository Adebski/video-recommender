package ztis.cassandra

import com.datastax.driver.core.policies.RoundRobinPolicy
import com.datastax.driver.core.{Cluster, ResultSet}
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.UserAndRating

class CassandraClient(private[cassandra] val config: CassandraConfiguration) extends StrictLogging {

  private val cluster = Cluster.builder().addContactPointsWithPorts(config.contactPoints)
    .withLoadBalancingPolicy(new RoundRobinPolicy)
    .build()
  private val session = cluster.connect()

  session.execute(CassandraClient.createKeyspaceQuery(config.keyspace))
  session.execute(CassandraClient.createRatingsTableQuery(config.keyspace, config.ratingsTableName))
  val preparedInsertToRatings = session.prepare(CassandraClient.insertToRatingsQuery(config.keyspace, config.ratingsTableName))

  def updateRating(userAndRating: UserAndRating): Unit = {
    val userID: java.lang.Integer = userAndRating.userID
    val videoID: java.lang.Integer = userAndRating.videoID
    val rating: java.lang.Integer = userAndRating.rating
    val timesUpvotedByFriends: java.lang.Integer = userAndRating.timesUpvotedByFriends

    val statement = preparedInsertToRatings.bind(userID,
      userAndRating.userOrigin.name,
      videoID,
      userAndRating.videoOrigin.toString,
      rating,
      timesUpvotedByFriends)
    session.execute(statement)
  }

  def clean(): Unit = {
    logger.info(s"Dropping keyspace ${config.keyspace}")
    session.execute(CassandraClient.dropKeyspaceQuery(config.keyspace))
  }

  def dropTable(keyspace: String, table: String): Unit = {
    logger.info(s"Dropping table $keyspace.$table")
    session.execute(CassandraClient.dropTableQuery(keyspace, table))
  }

  def allRatings: ResultSet = {
    session.execute(CassandraClient.selectAll(config.keyspace, config.ratingsTableName))
  }
}

object CassandraClient {

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
                                                            |"user_id" int,
                                                            |"user_origin" text,
                                                            |"video_id" int,
                                                            |"video_origin" text,
                                                            |"rating" int,
                                                            |"timesUpvotedByFriends" int,
                                                            |PRIMARY KEY(("user_id", "user_origin"), "video_id", "video_origin"))
     """.stripMargin

  def insertToRatingsQuery(keyspace: String, tableName: String): String =
    s"""
       |INSERT INTO "$keyspace"."$tableName" ("user_id", "user_origin", "video_id", "video_origin", "rating", "timesUpvotedByFriends") VALUES (?, ?, ?, ?, ?, ?)
     """.stripMargin

  def selectAll(keyspace: String, tableName: String): String = {
    s"""SELECT * FROM "$keyspace"."$tableName" """
  }
}
