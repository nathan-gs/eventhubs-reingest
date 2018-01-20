package gs.nathan.eventhubsreingest.eh

import java.time.Duration

import com.microsoft.azure.eventhubs._
import gs.nathan.eventhubsreingest.{Event, EventPublisher, Logger}

import scala.collection.JavaConverters._
import scala.util.Try

class EventHubPublisher(config: EventHubPublisherConfig) extends EventPublisher with Logger {

  /*
    A batch is normally 256kb. We can send max 1mb/s per partition and throughput unit.
    Let's sleep for 300ms after each batch.
  */
  val WaitAfterBatch = 300l * 1l

  def client() = {
    val connection = new ConnectionStringBuilder(config.ns, config.hub, config.keyName, config.keyValue)
    connection.setOperationTimeout(Duration.ofSeconds(60))
    val retryPolicy = new RetryExponential(
      Duration.ofSeconds(10),
      Duration.ofSeconds(60),
      10,
      "RETRY_WITH_10S_DELAY"
    )
    val ehClient = EventHubClient.createFromConnectionStringSync(connection.toString, retryPolicy)

    ehClient
  }

  override def numberOfPartitions = Try{
    val c = client()
    val count = c
      .getRuntimeInformation.get().getPartitionCount
    c.closeSync()
    count
  }

  override def send(partition: Int, events: Seq[Event], eventProperties: Map[String, String] = Map()) = Try{
    val ehClient = client()

    val batchOptions = new BatchOptions()
    batchOptions.partitionKey = partition.toString
    var batch = ehClient.createBatch(batchOptions)

    val toEvents = events.map(e => {
      val ev = new EventData(e.body)
      val properties = eventProperties + ("original_ts" -> e.ts.getTime.toString)
      ev.getProperties.putAll(properties.asJava)
      ev
    })

    toEvents.foreach(e => {
      if(!batch.tryAdd(e)) {
        ehClient.sendSync(batch)
        log.info(s"Batch for partition ${partition} sent of ${batch.getSize} msgs, sleeping for ${WaitAfterBatch}ms.")
        Thread.sleep(WaitAfterBatch)

        batch = ehClient.createBatch(batchOptions)
        batch.tryAdd(e)
      }
    })
    ehClient.sendSync(batch)
    ehClient.closeSync()
  }
}
