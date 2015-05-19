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
  
  it should "populate relationships table based on ratings table" in {
    // given
    val firstRating = UserVideoRating(10, UserOrigin.Twitter, 1, VideoOrigin.YouTube, 1)  
    val secondRating = UserVideoRating(10, UserOrigin.Twitter, 2, VideoOrigin.Vimeo, 2)  
    val thirdRating = UserVideoRating(10, UserOrigin.Twitter, 3, VideoOrigin.YouTube, 3)
    val userIDs = Vector(20, 30, 40)
    
    cassandraClient.updateRating(firstRating)
    cassandraClient.updateRating(secondRating)
    cassandraClient.updateRating(thirdRating)
    
    // when
    sparkCassandraClient.updateMoviesForNewRelationships(10, UserOrigin.Twitter, userIDs, UserOrigin.Twitter)
    
    // then
    val associations = sparkCassandraClient.userVideoImplicitAssociations.collect().toSet
    val expectedAssociations = Set(
      UserVideoImplicitAssociation(20, UserOrigin.Twitter, 10, UserOrigin.Twitter, 1, VideoOrigin.YouTube),
      UserVideoImplicitAssociation(30, UserOrigin.Twitter, 10, UserOrigin.Twitter, 1, VideoOrigin.YouTube),
      UserVideoImplicitAssociation(40, UserOrigin.Twitter, 10, UserOrigin.Twitter, 1, VideoOrigin.YouTube),
      UserVideoImplicitAssociation(20, UserOrigin.Twitter, 10, UserOrigin.Twitter, 2, VideoOrigin.Vimeo),
      UserVideoImplicitAssociation(30, UserOrigin.Twitter, 10, UserOrigin.Twitter, 2, VideoOrigin.Vimeo),
      UserVideoImplicitAssociation(40, UserOrigin.Twitter, 10, UserOrigin.Twitter, 2, VideoOrigin.Vimeo),
      UserVideoImplicitAssociation(20, UserOrigin.Twitter, 10, UserOrigin.Twitter, 3, VideoOrigin.YouTube),
      UserVideoImplicitAssociation(30, UserOrigin.Twitter, 10, UserOrigin.Twitter, 3, VideoOrigin.YouTube),
      UserVideoImplicitAssociation(40, UserOrigin.Twitter, 10, UserOrigin.Twitter, 3, VideoOrigin.YouTube)
    )
    assert(associations == expectedAssociations)
  }
  
  override def afterAll(): Unit = {
    spark.stop()
    
    super.afterAll()
  }
}
