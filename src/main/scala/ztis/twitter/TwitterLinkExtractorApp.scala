package ztis.twitter

import com.typesafe.config.ConfigFactory
import ztis.CassandraClient

object TwitterLinkExtractorApp extends App {
  val config = ConfigFactory.load("twitter-link-extractor")
  val cassandraClient = new CassandraClient(config)
  val extractor = new TwitterLinkExtractor(config, cassandraClient)
}
