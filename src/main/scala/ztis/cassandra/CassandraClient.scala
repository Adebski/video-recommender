package ztis.cassandra

import com.datastax.driver.core.policies.RoundRobinPolicy
import com.datastax.driver.core.{Cluster, ResultSet}
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.{UserVideoImplicitAssociation, UserVideoRating}

class CassandraClient(private[cassandra] val config: CassandraConfiguration) extends StrictLogging {

  private val cluster = Cluster.builder().addContactPointsWithPorts(config.contactPoints)
    .withLoadBalancingPolicy(new RoundRobinPolicy)
    .build()
  private val session = cluster.connect()

  session.execute(CassandraClient.createKeyspaceQuery(config.keyspace))
  session.execute(CassandraClient.createRatingsTableQuery(config.keyspace, config.ratingsTableName))
  session.execute(CassandraClient.createRelationshipsTableQuery(config.keyspace, config.relationshipsTableName))
  val preparedInsertToRatings = session.prepare(CassandraClient.insertToRatingsQuery(config.keyspace, config.ratingsTableName))
  val preparedInsertToRelationships = session.prepare(CassandraClient.insertToRelationshipsQuery(config.keyspace, config.relationshipsTableName))
  
  def updateRating(userVideoRating: UserVideoRating): Unit = {
    val userID: java.lang.Integer = userVideoRating.userID
    val videoID: java.lang.Integer = userVideoRating.videoID
    val rating: java.lang.Integer = userVideoRating.rating

    val statement = preparedInsertToRatings.bind(userID,
      userVideoRating.userOrigin.toString,
      videoID,
      userVideoRating.videoOrigin.toString,
      rating)
    session.execute(statement)
  }

  def updateImplicitAssociation(userVideoImplicitAssociation: UserVideoImplicitAssociation): Unit = {
    val userID: java.lang.Integer = userVideoImplicitAssociation.internalUserID
    val followedUserID: java.lang.Integer = userVideoImplicitAssociation.followedInternalUserID
    val videoID: java.lang.Integer = userVideoImplicitAssociation.internalVideoID
    
    val statement = preparedInsertToRelationships.bind(userID,
      userVideoImplicitAssociation.userOrigin.toString,
      followedUserID,
      userVideoImplicitAssociation.followedUserOrigin.toString,
      videoID,
      userVideoImplicitAssociation.videoOrigin.toString)
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

  def shutdown(): Unit = {
    session.close()
    cluster.close()
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
                                                            |PRIMARY KEY(("user_id", "user_origin"), "video_id", "video_origin"))
     """.stripMargin

  def createRelationshipsTableQuery(keyspace: String, tableName: String): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS "$keyspace"."$tableName" (
                                                            |"user_id" int,
                                                            |"user_origin" text,
                                                            |"video_id" int,
                                                            |"video_origin" text,
                                                            |"followed_user_id" int,
                                                            |"followed_user_origin" text,
                                                            |PRIMARY KEY(("user_id", "user_origin", "video_id", "video_origin"), "followed_user_origin", "followed_user_id"))
     """.stripMargin  
  }
  
  def insertToRatingsQuery(keyspace: String, tableName: String): String =
    s"""
       |INSERT INTO "$keyspace"."$tableName" ("user_id", "user_origin", "video_id", "video_origin", "rating") VALUES (?, ?, ?, ?, ?)
     """.stripMargin

  def insertToRelationshipsQuery(keyspace: String, tableName: String): String =
    s"""
       |INSERT INTO "$keyspace"."$tableName" ("user_id", "user_origin", "followed_user_id", "followed_user_origin", "video_id", "video_origin") VALUES (?, ?, ?, ?, ?, ?)
     """.stripMargin
  
  def selectAll(keyspace: String, tableName: String): String = {
    s"""SELECT * FROM "$keyspace"."$tableName" """
  }
}
