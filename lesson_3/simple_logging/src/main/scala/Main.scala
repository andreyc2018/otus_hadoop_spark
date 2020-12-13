import org.slf4j.LoggerFactory

object Main extends App {
  val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  logger.info("Log INFO message")
  logger.debug("Log DEBUG message")
  logger.error("Log ERROR message")
  logger.warn("Log WARN message")
  logger.trace("Log TRACE message")
  println("Just print")
}
