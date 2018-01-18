package gs.nathan.eventhubsreingest.eh

import com.microsoft.azure.eventhubs.{EventData, EventHubClient}
import gs.nathan.eventhubsreingest.{Event, EventPublisher, Logger}

import scala.collection.JavaConverters._
import scala.util.Try

class EventHubPublisher(config: EventHubPublisherConfig) extends EventPublisher with Logger {

  private val connectionString = config.connectionString
  val NumberOfMessagesToSendInBatch = 24
  val MaxBatchSize = 245760 - (1024 * 2)

  def client() = {
    val ehClient = EventHubClient.createFromConnectionStringSync(connectionString)
    ehClient.createBatch()
  }

  override def numberOfPartitions = Try{
    val ehClient = EventHubClient.createFromConnectionStringSync(connectionString)
    ehClient.getRuntimeInformation.get().getPartitionIds.length
  }

  override def send(partition: Int, events: Seq[Event], eventProperties: Map[String, String] = Map()) = Try{
    val ehClient = EventHubClient.createFromConnectionStringSync(connectionString)
    val partitionSender = ehClient.createPartitionSenderSync(partition.toString)
    val byteToEvents = events.map(e => {
      val ev = new EventData(e.body)
      val properties = eventProperties + ("original_ts" -> e.ts.getTime.toString)
      ev.getProperties.putAll(properties.asJava)
      ev
    })

    byteToEvents
      .grouped(NumberOfMessagesToSendInBatch)
      .foreach(s => {
        val size = s.map(e => e.getBytes.length).sum
        if(size < MaxBatchSize) {
          partitionSender.sendSync(s.asJava)
        } else {
          log.warn(s"Sending ${s.length} messages individually, because $size is bigger than $MaxBatchSize")
          s.foreach(e => partitionSender.sendSync(e))
        }
      })

  }
}
