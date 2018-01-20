package gs.nathan.eventhubsreingest.input

import org.apache.spark.SparkConf
import org.scalatest.WordSpec
import org.scalatest.Matchers._


class InputConfigBuilderSpec extends WordSpec {


  val baseConf = new SparkConf().setAll(Map("spark.test.unrelated" -> "bla"))
  val Prefix = "spark.eventhubreingest.inputs"

  "A InputConfigBuilder" when {
    "passed a SparkConf with no relevant properties" should {
      "return an empty Seq" in {
        InputConfigBuilder(baseConf, Prefix) shouldBe empty
      }
    }
    "passed a SparkConf with one spec, with alias [ttt]" should {

      "return a Seq[InputConfig]" in {
        val conf = baseConf
          .set(s"$Prefix.ttt.path", "wasbs://test/path")
          .set(s"$Prefix.ttt.format", "avro")

        InputConfigBuilder(conf, Prefix) should contain only (
          InputConfig("ttt", "wasbs://test/path", "avro", Map())
        )
      }
    }
  }

}
