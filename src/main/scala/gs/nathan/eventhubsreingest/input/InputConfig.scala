package gs.nathan.eventhubsreingest.input

import org.apache.spark.SparkConf

object InputConfig {
  def getByPrefix(sparkConf: SparkConf, prefix: String):Map[String, String] = {
    sparkConf.getAllWithPrefix(prefix)
      .toMap
      .map(v => (v._1.stripPrefix("."), v._2))
  }

  def apply(sparkConf: SparkConf, prefix: String):Seq[InputSpec] = {
    val appConf = getByPrefix(sparkConf, prefix)

    val keys = appConf
      .map(_._1.split('.')(0))
      .toSeq
      .distinct

    keys.map(key => {
      val conf = getByPrefix(sparkConf, s"$prefix.$key")
      InputSpec(key, conf)
    })

  }


}