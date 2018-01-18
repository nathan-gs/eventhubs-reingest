package gs.nathan.eventhubsreingest.eh

import com.microsoft.azure.eventhubs.ConnectionStringBuilder
import org.apache.spark.SparkConf

case class EventHubPublisherConfig(ns: String, hub: String, keyName: String, keyValue: String) {
  val connectionString:String = new ConnectionStringBuilder(ns, hub, keyName, keyValue).toString
}

object EventHubPublisherConfig {
  def apply(sparkConf: SparkConf, prefix: String):EventHubPublisherConfig = {
    val appConf = sparkConf.getAllWithPrefix(prefix)
      .toMap
      .map(v => (v._1.stripPrefix("."), v._2))

    EventHubPublisherConfig(
      appConfig(appConf, "ns"),
      appConfig(appConf, "name"),
      appConfig(appConf, "keyName"),
      appConfig(appConf, "keyValue")
    )
  }

  def appConfig(conf: Map[String, String], key: String): String = {
    conf.getOrElse(key, throw new RuntimeException(s"$key not set!"))
  }
}