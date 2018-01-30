package gs.nathan.eventhubsreingest.sql.udfs

import java.util.Locale

import org.apache.spark.sql.SparkSession
import java.util.TimeZone

class ToTimestamp extends Udf {

  val name = "ehcapture_timestamp"

  def register(spark: SparkSession):Unit = {
    spark.udf.register(name, ToTimestamp)
  }

  val ToTimestamp = (v: String) => {
    import java.sql.Timestamp
    import java.text.SimpleDateFormat //  1/12/2018 8:05:52 AM
    val dateFormat = new SimpleDateFormat("M/d/yyyy hh:mm:ss a", Locale.ROOT)

    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val parsedDate = dateFormat.parse(v)
    val timestamp = new Timestamp(parsedDate.getTime)

    timestamp
  }

}
