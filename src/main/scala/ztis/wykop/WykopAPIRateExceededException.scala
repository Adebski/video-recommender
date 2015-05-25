package ztis.wykop

import scala.concurrent.duration._

case class WykopAPIRateExceededException(error: WykopAPIError, waitFor: FiniteDuration = WykopAPI.DefaultWaitTime) 
  extends RuntimeException(s"Wykop API rate exceeded, $error, have to wait for $waitFor")
