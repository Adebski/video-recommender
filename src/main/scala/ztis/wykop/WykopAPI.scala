package ztis.wykop

import com.google.common.hash.Hashing
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.collection.mutable.ArrayBuffer
import scalaj.http.HttpOptions

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

  private def usersFollowingUserURL(userName: String, page: Int): String = {
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
    val entries = EntryMapper.toEntries(response.body)

    entries
  }

  def usersFollowingUser(userName: String): Vector[String] = {
    var page = 1
    var someFollowersLeft = true
    val buffer = ArrayBuffer.empty[String]

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

    buffer.toVector
  }

  private def downloadUsersFollowingUser(url: String, digest: String, buffer: ArrayBuffer[String]): ArrayBuffer[String] = {
    val response = scalaj.http.Http(url)
      .method("GET")
      .header("apisign", digest)
      .asString
    val usersFollowingUser = EntryMapper.toFollowers(response.body, buffer)

    usersFollowingUser
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
}
