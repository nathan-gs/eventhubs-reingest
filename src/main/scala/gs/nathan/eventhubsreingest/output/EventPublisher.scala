package gs.nathan.eventhubsreingest.output

import java.sql.Timestamp

import gs.nathan.eventhubsreingest.Event

import scala.util.Try

trait EventPublisher extends Serializable {

  def numberOfPartitions:Try[Int]

  def send(partition: Int, events: Seq[Event], eventProperties: Map[String, String] = Map()): Try[PublishResultStats]
}

case class PublishResultStats(numberOfEvents: Long, sizeInBytes: Long, numberOfBatches: Int, processingTimeInNanos: Long, startTs: Timestamp, endTs: Timestamp)