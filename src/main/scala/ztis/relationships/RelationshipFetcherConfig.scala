package ztis.relationships

import com.typesafe.config.Config

case class RelationshipFetcherConfig(wykopUsersTopic: String, 
                                     wykopUsersThreads: Int, 
                                     twitterUsersTopic: String, 
                                     twitterUsersThreads: Int)

object RelationshipFetcherConfig {
  def apply(config: Config): RelationshipFetcherConfig = {
    RelationshipFetcherConfig(config.getString("relationship-fetcher.wykop-users-topic"),
      config.getInt("relationship-fetcher.wykop-users-kafka-threads"),
      config.getString("relationship-fetcher.twitter-users-topic"),
      config.getInt("relationship-fetcher.twitter-users-kafka-threads"))  
  }
}
