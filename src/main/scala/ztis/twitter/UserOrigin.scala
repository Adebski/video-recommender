package ztis.twitter

sealed trait UserOrigin {
  def name: String
}

object Twitter extends UserOrigin {
  override def name: String = "twitter"
}