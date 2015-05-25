package ztis.wykop

case class WykopAPIException(error: WykopAPIError) extends RuntimeException(s"Error when using Wykop API, $error") {
  def exceededRate: Boolean = {
    error.code == WykopAPI.RateExceededErrorCode
  }
}
