package ztis.wykop

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import ztis.Initializer
import ztis.cassandra.{CassandraClient, CassandraConfiguration}
import ztis.relationships.KafkaRelationshipFetcherProducer

class WykopStreamInitializer extends Initializer {
  override def initialize(): Unit = {
    val config = ConfigFactory.load("wykop")
    val fetcherConfig = ConfigFactory.load("relationship-fetcher")
    val system = ActorSystem("ClusterSystem", config)
    val api = new WykopAPI(config)
    val cassandraConfig = CassandraConfiguration(config)
    val cassandraClient = new CassandraClient(cassandraConfig)
    val producer = new KafkaRelationshipFetcherProducer(fetcherConfig)

    val cluster = Cluster(system).registerOnMemberUp {
      logger.info("Creating WykopScrapperActor")
      val userServiceRouter = createUserServiceRouter(system)
      val videoServiceRouter = createVideoServiceRouter(system)
      val wykopScrapperActor =
        system.actorOf(WykopScrapperActor.props(api, cassandraClient, userServiceActor = userServiceRouter, videoServiceActor = videoServiceRouter, producer), "wykop-scrapper-actor")
    }
  }
}
