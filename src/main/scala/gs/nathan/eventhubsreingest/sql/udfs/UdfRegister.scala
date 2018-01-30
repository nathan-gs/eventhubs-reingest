package gs.nathan.eventhubsreingest.sql.udfs

import org.apache.spark.sql.SparkSession

object UdfRegister {

  def apply(spark: SparkSession, numberOfPartitions: Int): Unit = {
    Seq(
      new ToTimestamp(),
      new RandomPartition(numberOfPartitions),
      new ColumnToPartition(numberOfPartitions)
    ).foreach(u => u.register(spark))
  }
}
