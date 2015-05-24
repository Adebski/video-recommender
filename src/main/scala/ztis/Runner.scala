package ztis

object Runner extends App {
  val applicationName = args(0).toLowerCase
  setLoggerOutputName(applicationName)
  val initializer = GlobalInitializers.initializers(applicationName).apply()
  initializer.initialize()

  private def setLoggerOutputName(name: String): Unit = {
    System.setProperty("output-file", name)
  }
}
