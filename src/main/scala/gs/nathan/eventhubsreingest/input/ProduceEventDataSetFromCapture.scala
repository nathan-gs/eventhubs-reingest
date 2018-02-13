package gs.nathan.eventhubsreingest.input

import gs.nathan.eventhubsreingest.{Event, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Try

class ProduceEventDataSetFromCapture(
  spark: SparkSession,
  config: Seq[InputConfig]
) extends ProduceEventDataSet with Logger {

  def apply() = Try{
    log.info("No query given. Assuming EventHubs capture data.")

    val ds = spark
      .read
      .format(InputConfig.AvroFormat)
      .load(config.filter(_.sparkFormat == InputConfig.AvroFormat).map(_.path): _*)

    ds.createTempView("internal_capture_alias")
    val query =  """
                   |SELECT
                   |  ehcapture_timestamp(`EnqueuedTimeUtc`) as `ts`,
                   |  `Body` as `body`,
                   |  random_partition() as `eh_partition`
                   |FROM internal_capture_alias
                 """.stripMargin
    log.info("QUERY START")
    log.info(query)
    log.info("QUERY END")

    import spark.implicits._
    spark
      .sql(query)
      .as[Event]
  }

}
