package ztis.cassandra

import java.net.InetSocketAddress
import java.util

import com.typesafe.config.Config
import scala.collection.JavaConverters._

case class CassandraConfiguration(keyspace: String, 
                                  ratingsTableName: String, 
                                  relationshipsTableName: String,
                                  userFeaturesTableName: String, 
                                  productFeaturesTableName: String, 
                                  contactPoints: util.List[InetSocketAddress])

object CassandraConfiguration {
  def apply(config: Config): CassandraConfiguration = {
    CassandraConfiguration(config.getString("cassandra.keyspace"),
      config.getString("cassandra.ratings-table-name"),
      config.getString("cassandra.relationships-table-name"),
      config.getString("cassandra.user-features-table-name"),
      config.getString("cassandra.product-features-table-name"),
      config.getStringList("cassandra.contact-points").asScala.map(addressToInetAddress).asJava
    )  
  }

  private def addressToInetAddress(address: String): InetSocketAddress = {
    val hostAndPort = address.split(":")

    new InetSocketAddress(hostAndPort(0), hostAndPort(1).toInt)
  }
}
