package gs.nathan.eventhubsreingest.input

import gs.nathan.eventhubsreingest.{Event, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Try

class ProduceEventDataSetFromQuery(
  spark: SparkSession,
  config: Seq[InputConfig],
  query: String
) extends ProduceEventDataSet with Logger {

  def apply() = Try{
    config.foreach(i => {
      log.info(s"Reading ${i.alias}, from ${i.path} as ${i.format} with options: ${i.options}")
      val tbl = spark
        .read
        .format(i.format)
        .options(i.options)
        .load(i.path)
      tbl.createTempView(i.alias)
    })
    log.info("QUERY START")
    log.info(query)
    log.info("QUERY END")
    

    import spark.implicits._

    spark
      .sql(query)
      .as[Event]
  }

}
