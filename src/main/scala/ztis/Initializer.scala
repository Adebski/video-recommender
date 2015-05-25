package ztis

import com.typesafe.scalalogging.slf4j.StrictLogging
import ztis.util.RouterSupport

trait Initializer extends StrictLogging with RouterSupport {
  def initialize(): Unit
}
