package gs.nathan.eventhubsreingest.sql.udfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

trait Udf {

  def register(spark: SparkSession): Unit
  val name: String


}
