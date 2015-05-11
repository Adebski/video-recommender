package ztis.recommender

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import ztis.Spark
import ztis.cassandra.{CassandraClient, CassandraConfiguration, SparkCassandraClient}

class RecommenderService extends StrictLogging {
  private val cassandraConfig = CassandraConfiguration(ConfigFactory.load("model"))
  private val sparkConfig = SparkCassandraClient.setCassandraConfig(Spark.baseConfiguration("ModelBuilder"), cassandraConfig)
  private val sparkCassandraClient = new SparkCassandraClient(new CassandraClient(cassandraConfig), Spark.sparkContext(sparkConfig))
  private val model: MatrixFactorizationModel = sparkCassandraClient.fetchModel
  private val allUsers: Set[Int] = model.userFeatures.map(_._1).collect().toSet

  logger.info("Recommendation model successfully loaded.")

  def recommend(request: RecommendRequest): Either[Vector[Video], NoUserData.type] = {
    val usersWithData = filterUsers(request)

    if (usersWithData.isEmpty) {
      Right(NoUserData)
    }
    else {
      val howMany = 3

      val recommendations = usersWithData.map(userId => model.recommendProducts(userId, howMany))
        .flatten
        .sortBy(-_.rating)
        .map(rating => Video(rating.product.toString))
        .distinct
        .take(howMany)

      Left(recommendations)
    }
  }

  private def filterUsers(request: RecommendRequest) = {
    var usersWithData: Vector[Int] = Vector.empty

    val twitterId = request.twitterId.toInt // TODO(#16)
    if (haveDataFor(twitterId)) {
      usersWithData = usersWithData :+ twitterId
    }
    else {
      // TODO: put to the queue

      logger.info(s"Data not found. Scheduled twitter user ${request.twitterId} for high priority data retrieval")
    }

    val wykopId = 100 + request.wykopId.toInt // TODO(#16)
    if (haveDataFor(wykopId)) {
      usersWithData = usersWithData :+ wykopId
    }
    else {
      // TODO: put to the queue

      logger.info(s"Data not found. Scheduled wykop user ${request.wykopId} for high priority data retrieval")
    }

    usersWithData
  }

  private def haveDataFor(userId: Int) = {
    allUsers.contains(userId)
  }
}
