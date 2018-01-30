package gs.nathan.eventhubsreingest.sql.udfs

import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ColumnToPartitionSpec extends WordSpec {


  "A ColumnToPartition" when {
    "given a string" should {

      "never produce a number higher than the $partitionSize" in {

        val rUdf = new ColumnToPartition(12)
        rUdf.function("test") should be <=12
      }
    }
    "asked the name" should {
      "produce column_to_partition" in {
        val rUdf = new ColumnToPartition(12)
        rUdf.name shouldBe "column_to_partition"
      }
    }
  }
}
