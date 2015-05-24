package ztis.relationships

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
import ztis.wykop.WykopAPI

object RelationshipFetcherApp extends App with StrictLogging with RouterSupport {
  val relationshipFetcherConfig = ConfigFactory.load("relationship-fetcher")
  val twitterAuth = new OAuthAuthorization(new ConfigurationBuilder().setUseSSL(true).build())
  val internalTwitterAPI = (new TwitterFactory).getInstance(twitterAuth)
  val followersAPI = new TwitterFollowersAPI(internalTwitterAPI)
  val wykopAPI = new WykopAPI(relationshipFetcherConfig)
  
  val system = ActorSystem("ClusterSystem", relationshipFetcherConfig)
  val cassandraConfig = CassandraConfiguration(relationshipFetcherConfig)
  val cassandraClient = new CassandraClient(cassandraConfig)

  val sparkConfig = SparkCassandraClient.setCassandraConfig(Spark.baseConfiguration("RelationshipFetcher", uiPort = 4042), cassandraConfig)
  val sparkCassandraClient = new SparkCassandraClient(new CassandraClient(cassandraConfig), Spark.sparkContext(sparkConfig))

  val cluster = Cluster(system).registerOnMemberUp {
    logger.info("Creating relationship fetcher")
    val userServiceRouter = createUserServiceRouter(system)
    val fetcherActor = system.actorOf(RelationshipFetcherActor.props(followersAPI, wykopAPI, userServiceRouter, sparkCassandraClient))
    val relationshipFetcherConsumer = new KafkaRelationshipFetcherConsumer(relationshipFetcherConfig, fetcherActor)
    relationshipFetcherConsumer.subscribe()
  }
}
