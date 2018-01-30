package gs.nathan.eventhubsreingest.input

import java.sql.Timestamp
import javax.xml.bind.DatatypeConverter

import gs.nathan.eventhubsreingest.sql.udfs.{ColumnToPartition, RandomPartition, ToTimestamp, UdfRegister}
import gs.nathan.eventhubsreingest.SparkHelper
import org.scalatest.WordSpec
import org.scalatest.Matchers._

class ProduceEventDataSetFromQuerySpec extends WordSpec with SparkHelper {



  "A ProduceEventDataSetFromQuery" when {

    "called with a query using random_partition" should {

      "produce a Dataset[Event]" in {

        val spark = sparkSession()

        UdfRegister(spark, 2)

        val testData = Seq(
          TestEvent("2018-01-26T01:04:03.000Z", "blablabla"),
          TestEvent("2018-01-26T01:01:03.000Z", "blablabla"),
          TestEvent("2018-01-26T01:05:03.000Z", "blablabla")
        )

        import spark.implicits._
        val testDs = spark.createDataset(testData)

        testDs.createTempView("testds_random")
        /*
        Casting should be done automatically by Spark
         */
        val query =
          """
            |SELECT
            |  `ts`,
            |  `data` as `body`,
            |  random_partition() as `eh_partition`
            |FROM `testds_random`
            |ORDER BY
            | `ts`, `eh_partition`
          """.stripMargin
        val dsFactory = new ProduceEventDataSetFromQuery(spark, Seq(), query)

        val ds = dsFactory.apply().get

        ds
          .collect()
          .map(_.ts) shouldBe Array(
            toTs("2018-01-26T01:01:03.000Z"),
            toTs("2018-01-26T01:04:03.000Z"),
            toTs("2018-01-26T01:05:03.000Z")
          )

      }
    }

    "called with a query with a hash" should {

      "produce a Dataset[Event]" in {

        val spark = sparkSession()

        UdfRegister(spark, 2)

        val testData = Seq(
          TestEvent("2018-01-26T01:04:03.000Z", """{"id":"arandomid1", "k":"d"}"""),
          TestEvent("2018-01-26T01:01:03.000Z", """{"id":"arandomid2", "k":"d"}"""),
          TestEvent("2018-01-26T01:03:03.000Z", """{"id":"arandomid1", "k":"d"}""")
        )

        import spark.implicits._
        val testDs = spark.createDataset(testData)

        testDs.createTempView("testds_hash")
        /*
        Casting should be done automatically by Spark
         */
        val query =
          """
            |SELECT
            |  `ts`,
            |  `data` as `body`,
            |  column_to_partition(get_json_object(`data`, '$.id')) as `eh_partition`
            |FROM `testds_hash`
            |ORDER BY
            | `eh_partition`
          """.stripMargin
        val dsFactory = new ProduceEventDataSetFromQuery(spark, Seq(), query)

        val ds = dsFactory.apply().get

        ds
          .collect()
          .map(_.eh_partition) shouldBe Array(0,0,1)

      }
    }
  }

  def toTs(s: String) = new Timestamp(DatatypeConverter.parseDateTime(s: String).getTimeInMillis)

}

case class TestEvent(ts: String, data: String)