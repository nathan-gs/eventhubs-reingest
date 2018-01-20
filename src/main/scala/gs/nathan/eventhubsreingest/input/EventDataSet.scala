package gs.nathan.eventhubsreingest.input

import gs.nathan.eventhubsreingest.{Event, Logger}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object EventDataSet extends Logger {

  def apply(spark: SparkSession, config: Seq[InputConfig], query: Option[String]):Dataset[Event] = {
    import spark.implicits._
    val ds = query match {
      case Some(q) => fromQuery(spark, config, q)
      case _ => fromCapture(spark, config)
    }
    ds.as[Event]
  }

  private def fromQuery(spark: SparkSession, config: Seq[InputConfig], query: String):DataFrame = {
    spark.udf.register("ehcapture_timestamp", to_timestamp)

    config.foreach(i => {
      log.info(s"Reading ${i.alias}, from ${i.path} as ${i.format} with options: ${i.options}")
      val tbl = spark
        .read
        .format(i.format)
        .options(i.options)
        .load(i.path)
      tbl.createTempView(i.alias)
    })

    spark
      .sql(query)
  }

  private def fromCapture(spark: SparkSession, config: Seq[InputConfig]):DataFrame = {
    log.info("No query given. Assuming EventHubs capture data.")

    val ds = spark
      .read
      .format(InputConfig.AvroFormat)
      .load(config.filter(_.sparkFormat == InputConfig.AvroFormat).map(_.path): _*)
    val t = udf(to_timestamp)
    ds
      .select(t(col("EnqueuedTimeUtc")) as "ts", ds("Body") as "body")
  }

  private val to_timestamp = (v: String) => {
    import java.sql.Timestamp
    import java.text.SimpleDateFormat //  1/12/2018 8:05:52 AM
    val dateFormat = new SimpleDateFormat("M/d/yyyy hh:mm:ss a")
    val parsedDate = dateFormat.parse(v)
    val timestamp = new Timestamp(parsedDate.getTime)
    timestamp
  }

}


