package ztis.relationships

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import twitter4j.TwitterFactory
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import ztis.cassandra.{CassandraClient, CassandraConfiguration, SparkCassandraClient}
import ztis.wykop.WykopAPI
import ztis.{Initializer, Spark}

class RelationshipFetcherInitializer extends Initializer {
  override def initialize(): Unit = {
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
      val twitterFetcherActor = system.actorOf(TwitterRelationshipFetcherActor.props(followersAPI, userServiceRouter, sparkCassandraClient), "twitter-fetcher-actor")
      val wykopFetcherActor = system.actorOf(WykopRelationshipFetcherActor.props(wykopAPI, userServiceRouter, sparkCassandraClient), "wykop-fetcher-actor")
      val relationshipFetcherConsumer = new KafkaRelationshipFetcherConsumer(relationshipFetcherConfig,
        twitterRelationshipFetcher = twitterFetcherActor,
        wykopRelationshipFetcher = wykopFetcherActor)
      relationshipFetcherConsumer.subscribe()
    }
  }
}
