package ztis.wykop

import com.google.common.hash.Hashing
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.relationships.FollowersBuilder

import scala.collection.mutable.ArrayBuffer
import scalaj.http.HttpOptions

import scala.concurrent.duration._

object WykopAPI {
  val InitialPage = 1
  
  val FinalPage = 0
  
  val RateExceededErrorCode = 5
  
  val DefaultWaitTime = 60.minutes
}

class WykopAPI(config: Config) extends StrictLogging {
  private val baseURL = "http://a.wykop.pl/"

  private val appKey = config.getString("wykop.api-key")

  private val appSecret = config.getString("wykop.api-secret")

  private val md5 = Hashing.md5()

  private def mainPageURL(page: Int): String = {
    s"$baseURL/links/promoted/appkey,$appKey,page,$page,sort,day"
  }

  private def upcomingPageURL(page: Int): String = {
    s"$baseURL/links/upcoming/appkey,$appKey,page,$page,sort,date"
  }

  private def usersFollowingUserURL(userName: String, page: Long): String = {
    s"$baseURL/profile/followers/$userName/appkey,$appKey,userkey,,page,$page"
  }

  def mainPageEntries(page: Int = 1): Vector[Entry] = {
    downloadEntries(mainPageURL(1))
  }

  def upcomingPageEntries(page: Int = 1): Vector[Entry] = {
    downloadEntries(upcomingPageURL(page))
  }

  private def downloadEntries(url: String): Vector[Entry] = {
    val response = scalaj.http.Http(url)
      .option(HttpOptions.followRedirects(true))
      .method("GET")
      .asString
    val body = response.body
    val error = EntryMapper.toError(body)
    
    if (error.isDefined) {
      if (error.get.code == WykopAPI.RateExceededErrorCode) {
        throw new WykopAPIRateExceededException(error.get, WykopAPI.DefaultWaitTime)
      } else {
        throw new WykopAPIException(error.get)
      }
    } else {
      val entries = EntryMapper.toEntries(body)
      entries
    }
  }

  def usersFollowingUser(userName: String, partialResult: FollowersBuilder[String]): FollowersBuilder[String] = {
    var page = partialResult.page
    var someFollowersLeft = true
    val buffer = ArrayBuffer.empty[String]

    try {
      while (someFollowersLeft) {
        val oldSize = buffer.size
        val url = usersFollowingUserURL(userName, page)
        val digest = calculateDigest(url)

        downloadUsersFollowingUser(url, digest, buffer)
        val newSize = buffer.size

        if (oldSize == newSize) {
          someFollowersLeft = false
        } else {
          page += 1
        }
      }  
    } catch {
      case e: WykopAPIException => {
        if (e.exceededRate) {
          val newPartialResult = FollowersBuilder[String](page, partialResult.gatheredFollowers ++ buffer, partialResult = true) 
          throw new WykopFollowersFetchingLimitException(newPartialResult, e)  
        } else {
          throw e
        }
      }
    }
    
    FollowersBuilder[String](WykopAPI.FinalPage, partialResult.gatheredFollowers ++ buffer, partialResult = false)
  }

  private def downloadUsersFollowingUser(url: String, digest: String, buffer: ArrayBuffer[String]): Unit = {
    val response = scalaj.http.Http(url)
      .method("GET")
      .header("apisign", digest)
      .asString
    val body = response.body
    val error = EntryMapper.toError(body)

    if (error.isDefined) {
      throw new WykopAPIException(error.get)
    } else {
      EntryMapper.toFollowers(response.body, buffer)
    }
  }

  /**
   * Calculates message digest for Wykop API - does not support POST parameters
   */
  private def calculateDigest(url: String): String = {
    val len = url.length + appSecret.length
    val builder = new StringBuilder(capacity = len)

    builder.append(appSecret)
    builder.append(url)

    val hexDigest = md5.hashBytes(builder.mkString.getBytes()).toString

    hexDigest
  }
  
  private def recognizeError(error: WykopAPIError): Exception = {
    if (error.code == WykopAPI.RateExceededErrorCode) {
      new WykopAPIRateExceededException(error)
    } else {
      new WykopAPIException(error)
    }
  }
}
