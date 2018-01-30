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

    if(!query.toLowerCase.contains("order by")) {
      log.error("Query does not contain an ORDER BY")
      new RuntimeException("Query does not contain an ORDER BY")
    }

    import spark.implicits._

    spark
      .sql(query)
      .as[Event]
  }

}
