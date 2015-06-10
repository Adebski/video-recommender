package ztis

sealed trait VideoOrigin {
  def matches(url: String): Option[String]
}

object VideoOrigin {
  case object Vimeo extends VideoOrigin {
    // http://stackoverflow.com/questions/10488943/easy-way-to-get-vimeo-id-from-a-vimeo-url

    val vimeoRgx = """(?:https?://)?(?:www.)?(?:player.)?vimeo.com/(?:[a-z]*/)*([0-9]{6,11})[?]?.*"""
                    .replaceAll("\\s*#.*\n", "").replace(" ","").r

    override def matches(url: String): Option[String] = {
      url match {
        case vimeoRgx(a) => Some("http://vimeo.com/" + a)
        case _ => None
      }
    }

    override def toString: String = "vimeo"
  }
  
  case object YouTube extends VideoOrigin {
    // http://stackoverflow.com/questions/5830387/how-to-find-all-youtube-video-ids-in-a-string-using-a-regex
    // http://stackoverflow.com/questions/11431078/scala-youtube-video-id-from-url

    val youtubeRgx = """https?://         # Required scheme. Either http or https.
                       |(?:[0-9a-zA-Z-]+\.)? # Optional subdomain.
                       |(?:               # Group host alternatives.
                       |  youtu\.be/      # Either youtu.be,
                       || youtube\.com    # or youtube.com followed by
                       |  \S*             # Allow anything up to VIDEO_ID,
                       |  [^\w\-\s]       # but char before ID is non-ID char.
                       |)                 # End host alternatives.
                       |([\w\-]{11})      # $1: VIDEO_ID is exactly 11 chars.
                       |(?=[^\w\-]|$)     # Assert next char is non-ID or EOS.
                       |(?!               # Assert URL is not pre-linked.
                       |  [?=&+%\w.-]*      # Allow URL (query) remainder.
                       |  (?:             # Group pre-linked alternatives.
                       |    [\'"][^<>]*>  # Either inside a start tag,
                       |  | </a>          # or inside <a> element text contents.
                       |  )               # End recognized pre-linked alts.
                       |)                 # End negative lookahead assertion.
                       |[?=&+%\w.-]*       # Consume any URL (query) remainder.
                       |""".stripMargin.replaceAll("\\s*#.*\n", "").replace(" ","").r

    override def matches(url: String): Option[String] = {
      url match {
        case youtubeRgx(a) => Some("http://www.youtube.com/watch?v=" + a)
        case _ => None
      }
    }

    override def toString: String = "youtube"
  }
  
  case object MovieLens extends VideoOrigin {
    override def matches(url: String): Option[String] = None

    override def toString: String = "movielens"
  }
  
  val Origins = Set(Vimeo, YouTube, MovieLens)
  
  def recognize(url: String): Option[VideoOrigin] = {
    Origins.find(_.matches(url).isDefined)
  }
  
  def normalizeVideoUrl(url: String): Option[String] = {
    Origins.flatMap(_.matches(url)).headOption
  }
  
  def fromString(name: String): VideoOrigin = {
    name.toLowerCase match {
      case "youtube" => YouTube
      case "vimeo" => Vimeo
      case "movielens" => MovieLens
      case _ => throw new IllegalArgumentException(s"Unrecognized video origin $name")
    }
  }
}
