package ztis.twitter

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import twitter4j.TwitterFactory
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import ztis.Spark
import ztis.cassandra.{CassandraClient, CassandraConfiguration, SparkCassandraClient}
import ztis.util.RouterSupport

object RelationshipFetcherApp extends App with StrictLogging with RouterSupport {
  val twitterAuth = new OAuthAuthorization(new ConfigurationBuilder().setUseSSL(true).build())
  val internalTwitterAPI = (new TwitterFactory).getInstance(twitterAuth)
  val followersAPI = new FollowersAPI(internalTwitterAPI)
  val relationshipFetcherConfig = ConfigFactory.load("twitter-relationship-fetcher")
  val system = ActorSystem("ClusterSystem", relationshipFetcherConfig)
  val cassandraConfig = CassandraConfiguration(relationshipFetcherConfig)
  val cassandraClient = new CassandraClient(cassandraConfig)

  val sparkConfig = SparkCassandraClient.setCassandraConfig(Spark.baseConfiguration("RelationshipFetcher"), cassandraConfig)
  val sparkCassandraClient = new SparkCassandraClient(new CassandraClient(cassandraConfig), Spark.sparkContext(sparkConfig))

  val cluster = Cluster(system).registerOnMemberUp {
    logger.info("Creating relationship fetcher")
    val userServiceRouter = createUserServiceRouter(system)
    val fetcherActor = system.actorOf(RelationshipFetcherActor.props(followersAPI, userServiceRouter, sparkCassandraClient))
    val relationshipFetcherConsumer = new RelationshipFetcherConsumer(relationshipFetcherConfig, fetcherActor)
    relationshipFetcherConsumer.subscribe()
  }
}
