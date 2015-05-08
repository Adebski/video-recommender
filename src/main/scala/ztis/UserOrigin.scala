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
  
  def fromString(name: String): UserOrigin = {
    name.toLowerCase match {
      case "twitter" => Twitter
      case "movielens" => MovieLens
      case _ => throw new IllegalArgumentException(s"Unrecognized origin $name")
    }  
  }
}

