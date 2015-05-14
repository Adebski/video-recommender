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
  
  val Origins = Set(Vimeo, YouTube)
  
  def isKnownOrigin(host: String): Boolean = {
    Origins.exists(_.matches(host))
  }
}
