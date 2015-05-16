package ztis

sealed trait UserOrigin

object UserOrigin {
  case object Twitter extends UserOrigin 
  
  case object MovieLens extends UserOrigin 
  
  case object Wykop extends UserOrigin 
  
  def fromString(name: String): UserOrigin = {
    name.toLowerCase match {
      case "twitter" => Twitter
      case "movielens" => MovieLens
      case "wykop" => Wykop
      case _ => throw new IllegalArgumentException(s"Unrecognized user origin $name")
    }  
  }
}

