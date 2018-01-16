package gs.nathan.eventhubsreingest

import com.microsoft.azure.eventhubs.{ConnectionStringBuilder, EventData, EventHubClient}

import scala.collection.JavaConverters._
import scala.util.Try

class EventHubPublisher(ns: String, hub: String, keyName: String, keyValue: String) extends EventPublisher {

  val connectionString = new ConnectionStringBuilder(ns, hub, keyName, keyValue).toString
  val NumberOfMessagesToSendInBatch = 32

  override def numberOfPartitions = Try{
    val ehClient = EventHubClient.createFromConnectionStringSync(connectionString)
    ehClient.getRuntimeInformation.get().getPartitionIds.length
  }

  override def send(partition: Int, events: Seq[EventType], eventProperties: Map[String, String] = Map()) = Try{
    val ehClient = EventHubClient.createFromConnectionStringSync(connectionString)
    val partitionSender = ehClient.createPartitionSenderSync(partition.toString)
    val byteToEvents = events.map(e => {
      val ev = new EventData(e)
      ev.getProperties.putAll(eventProperties.asJava)
      ev
    })

    byteToEvents
      //.grouped(NumberOfMessagesToSendInBatch)
      .foreach(s => partitionSender.sendSync(s))

  }
}
