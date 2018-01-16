package gs.nathan.eventhubsreingest

import org.slf4j.LoggerFactory

trait Logger {

  val log = LoggerFactory.getLogger(getClass)
}
