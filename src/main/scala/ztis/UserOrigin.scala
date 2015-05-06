package ztis

sealed trait UserOrigin {
  def name: String
}

object UserOrigin {
  object Twitter extends UserOrigin {
    override def name: String = "twitter"
  }  
}

