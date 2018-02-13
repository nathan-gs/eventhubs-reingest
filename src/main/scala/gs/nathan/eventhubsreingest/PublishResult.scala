package gs.nathan.eventhubsreingest


case class PublishResult(partition: Int, stats: Option[PublishResultStats], exceptionClass: Option[String], exceptionMessage: Option[String]) {
  def isSuccess: Boolean = exceptionClass.isEmpty
}
