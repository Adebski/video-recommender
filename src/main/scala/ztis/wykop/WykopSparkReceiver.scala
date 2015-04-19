package ztis.wykop

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.duration._

class WykopSparkReceiver(config: Config, api: WykopAPI) extends Receiver[Entry](StorageLevel.MEMORY_ONLY) with StrictLogging {

  private val secondsBetweenRequests = config.getInt("seconds-between-requests").seconds
  
  private val scrapperThread = new Thread("wykopScrapper") {
    try {
      val mainPageEntries = api.mainPageEntries(1)
      val upcomingEntries = api.upcomingPageEntries(1)
      store(mainPageEntries.iterator)
      store(upcomingEntries.iterator)
    } catch {
      case e: Exception => logger.warn("Problems during wykop scrapping", e)
    }
    Thread.sleep(secondsBetweenRequests.toMillis)
  }

  override def onStart(): Unit = {
    scrapperThread.start()
  }

  override def onStop(): Unit = {
    
  }
}
