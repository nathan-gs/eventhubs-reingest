package gs.nathan.eventhubsreingest.input

import org.apache.spark.SparkConf

object InputConfigBuilder {
  def getByPrefix(sparkConf: SparkConf, prefix: String):Map[String, String] = {
    sparkConf.getAllWithPrefix(prefix)
      .toMap
      .map(v => (v._1.stripPrefix("."), v._2))
  }

  def apply(sparkConf: SparkConf, prefix: String):Seq[InputConfig] = {
    val appConf = getByPrefix(sparkConf, prefix)

    val keys = appConf
      .map(_._1.split('.')(0))
      .toSeq
      .distinct

    keys.map(key => {
      val conf = getByPrefix(sparkConf, s"$prefix.$key")
      apply(key, conf)
    })

  }

  def apply(alias: String, kv:Map[String, String]): InputConfig = {
    val path = kv("path")
    val format = kv("format")
    val options = kv.filterKeys(_.startsWith("options")).map(ov => {
      val key = ov._1.stripPrefix("options.")
      (key, ov._2)
    })

    InputConfig(alias, path, format, options)
  }

}

case class InputConfig(alias: String, path: String, format: String, options: Map[String, String]) {

  val sparkFormat:String = format match {
    case "avro" => InputConfig.AvroFormat
    case f => f
  }
}

object InputConfig {
  val AvroFormat="com.databricks.spark.avro"
}