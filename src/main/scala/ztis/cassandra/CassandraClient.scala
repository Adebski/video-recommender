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

  initializeDatabase()
  val preparedInsertToRatings = session.prepare(CassandraClient.insertToRatingsQuery(config.keyspace, config.ratingsTableName))
  val preparedInsertToRelationships = session.prepare(CassandraClient.insertToRelationshipsQuery(config.keyspace, config.relationshipsTableName))

  /**
   * Adds new rating for user, also adds new association to all users following given user.
   * 
   * This method assumes that all following users have the same userOrigin as user.
   * @param userVideoRating
   * @param usersFollowing
   */
  def updateRating(userVideoRating: UserVideoRating, usersFollowing: Vector[Int] = Vector.empty[Int]): Unit = {
    val userID: java.lang.Integer = userVideoRating.userID
    val userOrigin = userVideoRating.userOrigin.toString
    val videoID: java.lang.Integer = userVideoRating.videoID
    val videoOrigin = userVideoRating.videoOrigin.toString
    val rating: java.lang.Integer = userVideoRating.rating

    val statement = preparedInsertToRatings.bind(userID,
      userOrigin,
      videoID,
      videoOrigin,
      rating)
    session.execute(statement)
    
    usersFollowing.foreach { followingUserID =>
      updateImplicitAssociation(followingUserID, userOrigin, userID, userOrigin, videoID, videoOrigin) 
    }
  }

  def updateImplicitAssociation(userVideoImplicitAssociation: UserVideoImplicitAssociation): Unit = {
    val userID: java.lang.Integer = userVideoImplicitAssociation.internalUserID
    val followedUserID: java.lang.Integer = userVideoImplicitAssociation.followedInternalUserID
    val videoID: java.lang.Integer = userVideoImplicitAssociation.internalVideoID

    updateImplicitAssociation(userID,
      userVideoImplicitAssociation.userOrigin.toString,
      followedUserID,
      userVideoImplicitAssociation.followedUserOrigin.toString,
      videoID,
      userVideoImplicitAssociation.videoOrigin.toString)
  }
  
  private def updateImplicitAssociation(userID: java.lang.Integer, 
                                        userOrigin: String, 
                                        followedUserID: java.lang.Integer, 
                                        followedUserOrigin: String, 
                                        videoID: java.lang.Integer, 
                                        videoOrigin: String): Unit = {
    val statement = preparedInsertToRelationships.bind(userID,
      userOrigin,
      followedUserID,
      followedUserOrigin,
      videoID,
      videoOrigin)
    session.execute(statement)
  }
  
  private[cassandra] def initializeDatabase(): Unit = {
    session.execute(CassandraClient.createKeyspaceQuery(config.keyspace))
    session.execute(CassandraClient.createRatingsTableQuery(config.keyspace, config.ratingsTableName))
    session.execute(CassandraClient.createRelationshipsTableQuery(config.keyspace, config.relationshipsTableName))
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

  def allAssociations: ResultSet = {
    session.execute(CassandraClient.selectAll(config.keyspace, config.relationshipsTableName))
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
