package ztis

import java.net.InetSocketAddress

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.policies.RoundRobinPolicy
import com.typesafe.config.Config
import ztis.twitter.UserOrigin

import scala.collection.JavaConverters._

class CassandraClient(config: Config) {

  private val keyspace = config.getString("cassandra.keyspace")

  private val explicitAssocTableName = config.getString("cassandra.explicit-association-table-name")

  private val cluster = Cluster.builder().addContactPointsWithPorts(contactPoints)
    .withLoadBalancingPolicy(new RoundRobinPolicy)
    .build()
  private val session = cluster.connect()

  session.execute(CassandraClient.createKeyspaceQuery(keyspace))
  session.execute(CassandraClient.createExplicitTableQuery(keyspace, explicitAssocTableName))
  val preparedInsertToExplicit = session.prepare(CassandraClient.insertToExplicitQuery(keyspace, explicitAssocTableName))

  private def contactPoints: java.util.List[InetSocketAddress] = {
    config.getStringList("cassandra.contact-points").asScala.map(addressToInetAddress).asJava
  }

  private def addressToInetAddress(address: String): InetSocketAddress = {
    val hostAndPort = address.split(":")

    new InetSocketAddress(hostAndPort(0), hostAndPort(1).toInt)
  }

  def updateExplicitAssoc(user: String, origin: UserOrigin, link: String, rating: Int): Unit = {
    val statement = preparedInsertToExplicit.bind(user, origin.name, link, java.lang.Integer.valueOf(rating))
    session.execute(statement)
  }
}

object CassandraClient {

  def createKeyspaceQuery(keyspace: String): String =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """.stripMargin

  def createExplicitTableQuery(keyspace: String, tableName: String): String =
    s"""
       |CREATE TABLE IF NOT EXISTS $keyspace.$tableName (
                                                         | user_id text,
                                                         | user_origin text,
                                                         | link text,
                                                         | rating int,
                                                         | PRIMARY KEY((user_id, user_origin), link))
                                                         |
     """.stripMargin

  def insertToExplicitQuery(keyspace: String, tableName: String): String =
    s"""
       |INSERT INTO $keyspace.$tableName (user_id, user_origin, link, rating) VALUES (?, ?, ?, ?)
     """.stripMargin
}