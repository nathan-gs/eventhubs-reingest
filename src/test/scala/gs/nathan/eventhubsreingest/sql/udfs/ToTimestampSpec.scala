package gs.nathan.eventhubsreingest.sql.udfs


import java.sql.Timestamp
import java.util.{SimpleTimeZone, TimeZone}
import javax.xml.bind.DatatypeConverter

import org.scalatest.WordSpec
import org.scalatest.Matchers._

class ToTimestampSpec extends WordSpec {

  val data = Seq(
    ("1/12/2018 8:05:52 AM", "2018-01-12T08:05:52Z")
  )

  import java.sql.Timestamp
  import java.text.SimpleDateFormat //  1/12/2018 8:05:52 AM
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z")


  "A ToTimestamp" when {
    "a EventHub based timestring is given" should {

      "convert this to java.sql.Timestamp" in {
        val udf = new ToTimestamp()

        data.map{case (input, expected) => {

            udf.ToTimestamp(input) shouldBe new Timestamp(DatatypeConverter.parseDateTime(expected).getTimeInMillis)
            udf.name shouldBe "ehcapture_timestamp"
          }

        }
      }
    }
  }
}
