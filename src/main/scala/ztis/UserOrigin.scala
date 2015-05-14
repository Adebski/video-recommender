package ztis

sealed trait UserOrigin {
  def name: String
}

object UserOrigin {
  object Twitter extends UserOrigin {
    override def name: String = "twitter"
  }  
  
  object MovieLens extends UserOrigin {
    override def name: String  = "movielens"
  }
  
  object Wykop extends UserOrigin {
    override def name: String = "wykop"
  }
  
  def fromString(name: String): UserOrigin = {
    name.toLowerCase match {
      case "twitter" => Twitter
      case "movielens" => MovieLens
      case "wykop" => Wykop
      case _ => throw new IllegalArgumentException(s"Unrecognized user origin $name")
    }  
  }
}

