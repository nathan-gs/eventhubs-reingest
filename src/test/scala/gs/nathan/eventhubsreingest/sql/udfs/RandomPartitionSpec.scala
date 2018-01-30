package gs.nathan.eventhubsreingest.sql.udfs

import org.scalatest.Matchers._
import org.scalatest.WordSpec

class RandomPartitionSpec extends WordSpec {


  "A RandomPartiion" when {
    "a asked for a Random number between 0 and $partitionSize" should {

      "never produce a number higher than the $partitionSize" in {

        val rUdf = new RandomPartition(12)
        rUdf.RandomPartition() should be <=12
      }
    }
    "asked the name" should {
      "produce random_partition" in {
        val rUdf = new RandomPartition(12)
        rUdf.name shouldBe "random_partition"
      }
    }
  }
}
