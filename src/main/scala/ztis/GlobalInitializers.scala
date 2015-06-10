package ztis

import ztis.dump.CsvDumper
import ztis.model.ModelBuilderInitializer
import ztis.recommender.RecommenderInitializer
import ztis.relationships.RelationshipFetcherInitializer
import ztis.testdata.MovieLensDataLoaderInitializer
import ztis.twitter.{TweetProcessorInitializer, TwitterStreamInitializer}
import ztis.user_video_service.UserVideoServiceInitializer
import ztis.wykop.WykopStreamInitializer

object GlobalInitializers {
  val initializers = Map[String, () => Initializer](
    "csv-dump" -> (() => new CsvDumper
      ),
    "wykop-stream" -> (() => new WykopStreamInitializer),
    "model-builder" -> (() => new ModelBuilderInitializer),
    "recommender" -> (() => new RecommenderInitializer),
    "relationship-fetcher" -> (() => new RelationshipFetcherInitializer),
    "movie-lens-data-loader" -> (() => new MovieLensDataLoaderInitializer),
    "twitter-stream" -> (() => new TwitterStreamInitializer),
    "tweet-processor" -> (() => new TweetProcessorInitializer),
    "user-video-service" -> (() => new UserVideoServiceInitializer)
  )
}
