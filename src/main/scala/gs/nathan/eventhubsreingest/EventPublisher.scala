package gs.nathan.eventhubsreingest

import scala.util.Try

trait EventPublisher extends Serializable {

  def numberOfPartitions:Try[Int]

  def send(partition: Int, events: Seq[Event], eventProperties: Map[String, String] = Map()): Try[Unit]
}
