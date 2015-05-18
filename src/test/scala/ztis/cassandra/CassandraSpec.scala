package ztis.cassandra

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class CassandraSpec(val config: Config) extends FlatSpec with BeforeAndAfterAll with StrictLogging {
  val cassandraConfig = CassandraConfiguration(config)
  val cassandraClient = new CassandraClient(cassandraConfig)

  override def afterAll(): Unit = {
    logger.info("Shutting down and cleaning Cassandra")
    cassandraClient.clean()
    cassandraClient.shutdown()
    super.afterAll()
  }
}
