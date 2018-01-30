package gs.nathan.eventhubsreingest.sql.udfs
import org.apache.spark.sql.SparkSession
import scala.util.Random

class RandomPartition(partitionSize: Int) extends Udf {

  def register(spark: SparkSession):Unit = {
      spark.udf.register(name, this.RandomPartition)
  }

  val name: String = "random_partition"


  val RandomPartition = () => {
    Random.nextInt(partitionSize + 1)
  }
}
