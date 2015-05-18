package ztis.cassandra

import com.typesafe.config.ConfigFactory
import ztis._

class SparkCassandraClientTest extends CassandraSpec(ConfigFactory.load("cassandra.conf")) {
  val sparkConf = Spark.baseConfiguration("SparkCassandraClientTest")
  SparkCassandraClient.setCassandraConfig(sparkConf, cassandraConfig)
  val spark = Spark.sparkContext(conf = sparkConf)
  val sparkCassandraClient = new SparkCassandraClient(cassandraClient, spark)
  
  "SparkCassandraClient" should "retrieve UserVideoFullInformation" in {
    // given
    // movie rated by first user
    val firstRating = UserVideoRating(1, UserOrigin.Twitter, 1, VideoOrigin.YouTube, 1)

    // movie rated by second user followed by first user
    val secondRating = UserVideoRating(2, UserOrigin.Twitter, 2, VideoOrigin.Vimeo, 1)
    val firstAssociation = UserVideoImplicitAssociation(1, UserOrigin.Twitter, 2, UserOrigin.Twitter, 2, VideoOrigin.Vimeo)
    
    // movie rated by first and third user, first user follows third user
    val thirdRating = UserVideoRating(1, UserOrigin.Twitter, 3, VideoOrigin.YouTube, 1)
    val fourthRating = UserVideoRating(3, UserOrigin.Twitter, 3, VideoOrigin.YouTube, 1)
    val secondAssociation = UserVideoImplicitAssociation(1, UserOrigin.Twitter, 3, UserOrigin.Twitter, 3, VideoOrigin.YouTube)
    
    cassandraClient.updateRating(firstRating)
    
    cassandraClient.updateRating(secondRating)
    cassandraClient.updateImplicitAssociation(firstAssociation)
    
    cassandraClient.updateRating(thirdRating)
    cassandraClient.updateRating(fourthRating)
    cassandraClient.updateImplicitAssociation(secondAssociation)
    
    // when
    val userVideoRatings = sparkCassandraClient.userVideoRatingsRDD.collect().toSet
    val userAssociations = sparkCassandraClient.userVideoImplicitAssociations.collect().toSet
    val userVideoFullInformation = sparkCassandraClient.userVideoFullInformation.collect().toSet
    val expectedUserVideoFullInformation = Set(
      UserVideoFullInformation(1, UserOrigin.Twitter, 1, VideoOrigin.YouTube, 1, 0),
      UserVideoFullInformation(1, UserOrigin.Twitter, 2, VideoOrigin.Vimeo, 0, 1),
      UserVideoFullInformation(1, UserOrigin.Twitter, 3, VideoOrigin.YouTube, 1, 1),
      UserVideoFullInformation(2, UserOrigin.Twitter, 2, VideoOrigin.Vimeo, 1, 0),
      UserVideoFullInformation(3, UserOrigin.Twitter, 3, VideoOrigin.YouTube, 1, 0)
    )
    
    assert(userVideoRatings == Set(firstRating, secondRating, thirdRating, fourthRating))
    assert(userAssociations == Set(firstAssociation, secondAssociation))
    assert(userVideoFullInformation == expectedUserVideoFullInformation)
  } 
  
  override def afterAll(): Unit = {
    spark.stop()
    
    super.afterAll()
  }
}
