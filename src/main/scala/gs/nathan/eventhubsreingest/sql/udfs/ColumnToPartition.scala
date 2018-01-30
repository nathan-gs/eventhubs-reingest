package gs.nathan.eventhubsreingest.sql.udfs
import org.apache.spark.sql.SparkSession

class ColumnToPartition(partitionSize: Int) extends Udf {
  override def register(spark: SparkSession): Unit = {
    spark.udf.register(name, this.function)
  }

  val function = (value: Any) => {
    value.toString.hashCode % partitionSize
  }

  override val name: String = "column_to_partition"
}
