package gs.nathan.eventhubsreingest.input

import org.scalatest.WordSpec
import org.scalatest.Matchers._



class InputConfigSpec  extends WordSpec {

  "A InputConfig" when {
    "format is set to [avro]" should {
      "return [com.databricks.spark.avro]" in {
        InputConfig("ALIAS", "PATH", "avro", Map()).sparkFormat shouldBe "com.databricks.spark.avro"
      }
    }

    "format is set to something else than [avro]" should {
      "return the exact input" in {
        InputConfig("ALIAS", "PATH", "testformat", Map()).sparkFormat shouldBe "testformat"
      }
    }
  }
}
