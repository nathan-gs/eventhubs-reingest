package gs.nathan.eventhubsreingest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, col}

object Main extends Logger {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Avro to Event Hubs")
      .getOrCreate()

    val appConf = config(spark)

    import com.databricks.spark.avro._
    val df = spark.read.avro(appConfig(appConf, "input.path"))

    val mappedToTs = df.withColumn("ts", to_timestamp(col("EnqueuedTimeUtc")))


    val publisher = new EventHubPublisher(
      appConfig(appConf, "output.eh.ns"),
      appConfig(appConf, "output.eh.name"),
      appConfig(appConf, "output.eh.keyName"),
      appConfig(appConf, "output.eh.keyValue"))

    val toEventHub = new DataFrameToEventHub(publisher)

    val orderByColumns = appConfig(appConf, "input.orderBy").split(",").toSeq

    val status = toEventHub.apply(mappedToTs, ColSpec(ts = "ts"), orderByColumns)
    status
      .filter(_.isFailure)
      .foreach(t => log.error("Failed to publish events. ", t.failed.get))
  }

  def config(spark: SparkSession):Map[String, String] = {
    spark
      .sparkContext
      .getConf
      .getAllWithPrefix("spark.eventhubsreingest")
      .toMap
      .map(v => (v._1.stripPrefix("."), v._2))
  }

  def appConfig(conf: Map[String, String], key: String): String = {
    conf.getOrElse(key, throw new RuntimeException(s"$key not set!"))
  }

  def to_timestamp=udf((v: String) => {
    import java.sql.Timestamp
    import java.text.SimpleDateFormat //  1/12/2018 8:05:52 AM
    val dateFormat = new SimpleDateFormat("M/d/yyyy hh:mm:ss a")
    val parsedDate = dateFormat.parse(v)
    val timestamp = new Timestamp(parsedDate.getTime)
    timestamp
  })
}
