package ztis

sealed trait VideoOrigin {
  def matches(host: String): Boolean
}

object VideoOrigin {
  case object Vimeo extends VideoOrigin {
    private val domains = Set("vimeo.com", "www.vimeo.com")

    override def matches(host: String): Boolean = {
      domains.contains(host)
    }
  }
  
  case object YouTube extends VideoOrigin {
    private val domains = Set("youtube.com", "www.youtube.com")
    
    override def matches(host: String): Boolean = {
      domains.contains(host)
    }
  }
  
  case object MovieLens extends VideoOrigin {
    override def matches(host: String): Boolean = false
  }
  
  val Origins = Set(Vimeo, YouTube, MovieLens)
  
  def recognize(host: String): Option[VideoOrigin] = {
    Origins.filter(_.matches(host)).headOption
  }
  
  def fromString(name: String): VideoOrigin = {
    name.toLowerCase match {
      case "youtube" => YouTube
      case "viemo" => Vimeo
      case "movielens" => MovieLens
      case _ => throw new IllegalArgumentException(s"Unrecognized video origin $name")
    }
  }
}
