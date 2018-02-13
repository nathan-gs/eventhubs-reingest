package gs.nathan.eventhubsreingest

import java.sql.Timestamp
import java.time.Duration

import org.scalatest.WordSpec
import org.scalatest.Matchers._

class TimestampRangeSpec extends WordSpec {

  "A TimestampRange" when {
    "asked for a range of times" should {
      val startTs = 1000
      val endTs = 5000
      val range = TimestampRange(new Timestamp(startTs), new Timestamp(endTs), Duration.ofSeconds(1))

      "return tuples of Timestamp" in {


        range.head shouldBe (new Timestamp(startTs), new Timestamp(startTs + 1000))
        range.last shouldBe (new Timestamp(endTs), new Timestamp(endTs + 1000))
        range.size shouldBe 5
        range shouldBe Seq((1000, 2000), (2000,3000), (3000,4000), (4000,5000), (5000,6000)).map(v => (new Timestamp(v._1), new Timestamp(v._2)))
      }

      "be iteratable more than once" in {

        range.isTraversableAgain shouldBe true
        range shouldBe range
      }
    }

  }
}
