package ztis

sealed trait UserOrigin {
  def name: String
}

object UserOrigin {
  object Twitter extends UserOrigin {
    override def name: String = "twitter"
  }  
  
  def fromString(name: String): UserOrigin = {
    name.toLowerCase match {
      case "twitter" => Twitter
      case _ => throw new IllegalArgumentException(s"Unrecognized origin $name")
    }  
  }
}

