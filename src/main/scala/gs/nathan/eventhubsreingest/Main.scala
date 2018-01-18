package gs.nathan.eventhubsreingest

import gs.nathan.eventhubsreingest.eh.{EventHubPublisher, EventHubPublisherConfig}
import gs.nathan.eventhubsreingest.input.{EventDataSet, InputConfig}
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

    val ds = EventDataSet(spark, InputConfig(sparkConf, s"$ConfigPrefix.inputs"), sparkConf.getOption(s"$ConfigPrefix.query"))
    sparkConf.getOption(s"$ConfigPrefix.cache") match {
      case Some("false") =>
      case _ => ds.cache()
    }

    val publisherConfig = EventHubPublisherConfig(sparkConf, s"$ConfigPrefix.output.eh")
    val publisher = new EventHubPublisher(publisherConfig)

    val toEventHub = new EventDataSetToEventHub(publisher, spark)

    val status = toEventHub.apply(ds)

    spark.stop()

    if(status.exists(_.isFailure)) {
      status
        .filter(_.isFailure)
        .foreach(t => log.error("Failed to publish events.", t.failed.get))
      System.exit(1)
    }
  }


}

