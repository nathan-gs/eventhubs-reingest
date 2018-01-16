package gs.nathan.eventhubsreingest

import scala.util.Try

trait EventPublisher extends Serializable {

  type EventType = Array[Byte]

  def numberOfPartitions:Try[Int]

  def send(partition: Int, events: Seq[EventType], eventProperties: Map[String, String] = Map()): Try[Unit]
}
