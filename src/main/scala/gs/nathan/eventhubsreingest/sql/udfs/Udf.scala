package gs.nathan.eventhubsreingest.sql.udfs

import org.apache.spark.sql.SparkSession

trait Udf extends Serializable {

  def register(spark: SparkSession): Unit
  val name: String


}
