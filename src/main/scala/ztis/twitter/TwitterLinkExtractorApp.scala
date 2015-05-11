package ztis.twitter

import com.typesafe.config.ConfigFactory
import ztis.cassandra.{CassandraConfiguration, CassandraClient}

object TwitterLinkExtractorApp extends App {
  val config = ConfigFactory.load("twitter-link-extractor")
  val cassandraConfig = CassandraConfiguration(config)
  val cassandraClient = new CassandraClient(cassandraConfig)
  val extractor = new TwitterLinkExtractor(config, cassandraClient)
}
