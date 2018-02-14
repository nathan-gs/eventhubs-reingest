package gs.nathan.eventhubsreingest

import gs.nathan.eventhubsreingest.output.PublishResultStats


case class PublishResult(partition: Int, stats: Option[PublishResultStats], exceptionClass: Option[String], exceptionMessage: Option[String]) {
  def isSuccess: Boolean = exceptionClass.isEmpty
}
