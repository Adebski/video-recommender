package ztis

import java.net.{InetSocketAddress, InetAddress}

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.policies.RoundRobinPolicy
import com.typesafe.config.Config

import scala.collection.JavaConverters._

class CassandraClient(config: Config) {
  
  private val cluster = Cluster.builder().addContactPointsWithPorts(contactPoints)
    .withLoadBalancingPolicy(new RoundRobinPolicy)
    .build()
  private val session = cluster.connect()
  
  session.execute(CassandraClient.CreateKeyspace)
  session.execute(CassandraClient.CreateExplicit)
  val preparedInsertToExplicit = session.prepare(CassandraClient.InsertToExplicit)
  
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
  
  val Keyspace = "recommender"
  
  val ExplicitAssocTableName = "ExplicitAssoc"
                                
  val CreateKeyspace =
    s"""
      |CREATE KEYSPACE IF NOT EXISTS $Keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}
    """.stripMargin
  
  val CreateExplicit =
    s"""
       |CREATE TABLE IF NOT EXISTS $Keyspace.$ExplicitAssocTableName (
       |  user_id text,
       |  user_origin text,
       |  link text,
       |  rating int,
       |  PRIMARY KEY((user_id, user_origin), link))
       |
     """.stripMargin
  
  val InsertToExplicit =
    s"""
       |INSERT INTO $Keyspace.$ExplicitAssocTableName (user_id, user_origin, link, rating) VALUES (?, ?, ?, ?)
     """.stripMargin
}