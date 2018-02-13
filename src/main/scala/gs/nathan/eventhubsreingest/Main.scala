package gs.nathan.eventhubsreingest

import gs.nathan.eventhubsreingest.eh.{EventHubPublisher, EventHubPublisherConfig}
import gs.nathan.eventhubsreingest.input.{ProduceEventDataSetFromCapture, ProduceEventDataSetFromQuery, InputConfigBuilder}
import gs.nathan.eventhubsreingest.sql.udfs.{RandomPartition, ToTimestamp, UdfRegister}
import org.apache.spark.sql.SparkSession

object Main extends Logger {

  val ConfigPrefix = "spark.eventhubsreingest"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Avro to EventHubs")
      .getOrCreate()

    val sparkConf = spark.sparkContext.getConf

    log.info(s"$ConfigPrefix START")
    log.info(sparkConf.toDebugString)
    log.info(s"$ConfigPrefix END")

    val publisherConfig = EventHubPublisherConfig(sparkConf, s"$ConfigPrefix.output.eh")
    val publisher = new EventHubPublisher(publisherConfig)

    UdfRegister(spark, publisher.numberOfPartitions.get)

    val produceEventDataSet = sparkConf.getOption(s"$ConfigPrefix.query") match {
      case Some(q) => new ProduceEventDataSetFromQuery(spark, InputConfigBuilder(sparkConf, s"$ConfigPrefix.inputs"), q)
      case _ => new ProduceEventDataSetFromCapture(spark, InputConfigBuilder(sparkConf, s"$ConfigPrefix.inputs"))
    }

    val ds = produceEventDataSet.apply().get
    sparkConf.getOption(s"$ConfigPrefix.cache") match {
      case Some("false") =>
      case _ => ds.cache()
    }

    val toEventHub = new EventDataSetToEventPublisher(publisher, spark)

    val status = toEventHub.apply(ds)

    //spark.stop()


    if(status.exists(!_.isSuccess)) {
     status
        .filter(!_.isSuccess)
        .foreach(t => log.error(s"Failed to publish events, partition ${t.partition}, exception ${t.exceptionClass.getOrElse("")}, message: ${t.exceptionMessage.getOrElse("")}"))
      System.exit(1)
    } else {
      println("--------------- STATS ---------------")
      status
        .groupBy(_.partition)
        .toList
        .sortBy(_._1)
        .foreach {case (partition, statsO) =>
          val stats = statsO.filter(_.stats.isDefined).map(_.stats.get)
          println(s"Partition $partition # Events: ${stats.map(_.numberOfEvents).sum}, # Batches: ${stats.map(_.numberOfBatches).sum}, Size (Bytes): ${stats.map(_.sizeInBytes).sum}")
      }
    }

  }


}

