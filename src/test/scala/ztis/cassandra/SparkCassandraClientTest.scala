package ztis.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import ztis._

class SparkCassandraClientTest extends CassandraSpec(ConfigFactory.load("cassandra.conf")) {
  val sparkConf = Spark.baseConfiguration("SparkCassandraClientTest")
  SparkCassandraClient.setCassandraConfig(sparkConf, cassandraConfig)
  val spark = Spark.sparkContext(conf = sparkConf)
  val sparkCassandraClient = new SparkCassandraClient(cassandraClient, spark)
  
  "SparkCassandraClient" should "retrieve UserVideoFullInformation" in {
    // given
    val firstRating = UserVideoRating(1, UserOrigin.Twitter, 1, VideoOrigin.YouTube, 1)
    val secondRating = UserVideoRating(2, UserOrigin.Twitter, 2, VideoOrigin.Vimeo, 1)
    val thirdRating = UserVideoRating(3, UserOrigin.Twitter, 3, VideoOrigin.YouTube, 1)
    val fourthRating = UserVideoRating(1, UserOrigin.Twitter, 3, VideoOrigin.YouTube, 1)
    
    cassandraClient.updateRating(firstRating)
    // first user follows second user
    cassandraClient.updateRating(secondRating, Vector(1))
    // first user follows third user
    cassandraClient.updateRating(thirdRating, Vector(1))
    cassandraClient.updateRating(fourthRating)
    
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
    val expectedAssociations = Set(UserVideoImplicitAssociation(1, UserOrigin.Twitter, 2, UserOrigin.Twitter, 2, VideoOrigin.Vimeo),
      UserVideoImplicitAssociation(1, UserOrigin.Twitter, 3, UserOrigin.Twitter, 3, VideoOrigin.YouTube)
    )
    
    assert(userVideoRatings == Set(firstRating, secondRating, fourthRating, thirdRating))
    assert(userAssociations == expectedAssociations)
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
