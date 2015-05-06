package ztis.wykop

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.codehaus.jackson.map.ObjectMapper

import scalaj.http.HttpOptions

class WykopAPI(config: Config) extends StrictLogging {
  private val baseURL = "http://a.wykop.pl/"

  private val appKey = config.getString("wykop.api-key")

  private val mapper = new ObjectMapper()

  private def mainPageURL(page: Int): String = {
    s"$baseURL/links/promoted/appkey,$appKey,page,$page,sort,day"
  }

  private def upcomingPageURL(page: Int): String = {
    s"$baseURL/links/upcoming/appkey,$appKey,page,$page,sort,date"  
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
}
