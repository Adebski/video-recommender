package ztis

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}


class AkkaSpec extends TestKit(ActorSystem("test-actor-system", ConfigFactory.load("akka"))) with FlatSpecLike with MockitoSugar with ImplicitSender with BeforeAndAfterAll with StrictLogging {
  override protected def afterAll(): Unit = {
    logger.info(s"Shutting down $system")
    system.shutdown()
    super.afterAll()
  }
}
