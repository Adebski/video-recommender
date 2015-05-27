package ztis

import org.scalatest.FlatSpec
import ztis.VideoOrigin.{YouTube, Vimeo}

class VideoOriginTest extends FlatSpec  {
  "Vimeo" should "recognize good links" in {
    val goodLinks = Map(
      "http://vimeo.com/6701902" -> "http://vimeo.com/6701902",
      "http://vimeo.com/670190233" -> "http://vimeo.com/670190233",
      "http://player.vimeo.com/video/67019023" -> "http://vimeo.com/67019023",
      "http://player.vimeo.com/video/6701902" -> "http://vimeo.com/6701902",
      "http://player.vimeo.com/video/67019022?title=0&amp;byline=0&amp;portrait=0" -> "http://vimeo.com/67019022",
      "http://player.vimeo.com/video/6719022?title=0&amp;byline=0&amp;portrait=0" -> "http://vimeo.com/6719022",
      "http://vimeo.com/channels/vimeogirls/6701902" -> "http://vimeo.com/6701902",
      "http://vimeo.com/channels/vimeogirls/67019023" -> "http://vimeo.com/67019023",
      "http://vimeo.com/channels/staffpicks/67019026" -> "http://vimeo.com/67019026",
      "http://vimeo.com/15414122" -> "http://vimeo.com/15414122",
      "http://vimeo.com/channels/vimeogirls/66882931" -> "http://vimeo.com/66882931"
    )

    goodLinks.foreach { case (link, expectedNormalizedLink) =>
      assert(Vimeo.matches(link) == Some(expectedNormalizedLink), s"problem with $link")
    }
  }

  it should "discard bad links" in {
    val badLinks = Vector(
      "http://vimeo.com/videoschool",
      "http://vimeo.com/videoschool/archive/behind_the_scenes",
      "http://vimeo.com/forums/screening_room",
      "http://vimeo.com/forums/screening_room/topic:42708",
      "http://www.youtube.com/watch?v=NLqAF9hrVbY"
    )

    badLinks.foreach { link =>
      assert(Vimeo.matches(link).isEmpty, s"problem with $link")
    }
  }

  "Youtube" should "recognize good links" in {
    val goodLinks = Map(
      "http://youtu.be/NLqAF9hrVbY" -> "http://www.youtube.com/watch?v=NLqAF9hrVbY",
      "http://www.youtube.com/embed/NLqAF9hrVbY" -> "http://www.youtube.com/watch?v=NLqAF9hrVbY",
      "https://www.youtube.com/embed/NLqAF9hrVbY" -> "http://www.youtube.com/watch?v=NLqAF9hrVbY",
      "http://www.youtube.com/v/NLqAF9hrVbY?fs=1&hl=en_US" -> "http://www.youtube.com/watch?v=NLqAF9hrVbY",
      "http://www.youtube.com/v/NLqAF9hrVbY?fs=1&hl=en_US" -> "http://www.youtube.com/watch?v=NLqAF9hrVbY",
      "http://www.youtube.com/watch?v=NLqAF9hrVbY" -> "http://www.youtube.com/watch?v=NLqAF9hrVbY",
      "https://www.youtube.com/watch?v=NLqAF9hrVbY" -> "http://www.youtube.com/watch?v=NLqAF9hrVbY",
      "http://www.youtube.com/user/Scobleizer#p/u/1/1p3vcRhsYGo" -> "http://www.youtube.com/watch?v=1p3vcRhsYGo",
      "http://www.youtube.com/ytscreeningroom?v=NRHVzbJVx8I" -> "http://www.youtube.com/watch?v=NRHVzbJVx8I",
      "http://www.youtube.com/sandalsResorts#p/c/54B8C800269D7C1B/2/PPS-8DMrAn4" -> "http://www.youtube.com/watch?v=PPS-8DMrAn4",
      "http://gdata.youtube.com/feeds/api/videos/NLqAF9hrVbY" -> "http://www.youtube.com/watch?v=NLqAF9hrVbY",
      "http://www.youtube.com/watch?v=spDj54kf-vY&feature=g-vrec" -> "http://www.youtube.com/watch?v=spDj54kf-vY",
      "http://www.youtube.com/watch?v=spDj54kf-vY&feature=youtu.be" -> "http://www.youtube.com/watch?v=spDj54kf-vY"
    )

    goodLinks.foreach { case (link, expectedNormalizedLink) =>
      assert(YouTube.matches(link) == Some(expectedNormalizedLink), s"problem with $link")
    }
  }

  it should "discard bad links" in {
    val badLinks = Vector(
      "https://www.youtube.com/",
      "https://www.youtube.com/playlist?list=WL",
      "https://www.youtube.com/playlist?list=PL2pzgHdrZzwY_UkORbQqcrO1eyi4T-dsZ",
      "https://www.youtube.com/channel/UCRZ3n9oDGbRVXuQKpySIpNg",
      "https://www.youtube.com/user/Tytuzmuzik",
      "http://vimeo.com/670190233"
    )

    badLinks.foreach { link =>
      assert(YouTube.matches(link).isEmpty, s"problem with $link")
    }
  }
}
