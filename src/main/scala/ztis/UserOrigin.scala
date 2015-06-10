package ztis

sealed trait UserOrigin

object UserOrigin {
  case object Twitter extends UserOrigin {
    override def toString: String = "twitter"
  }
  
  case object MovieLens extends UserOrigin {
    override def toString: String = "movielens"
  }
  
  case object Wykop extends UserOrigin {
    override def toString: String = "wykop"
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

