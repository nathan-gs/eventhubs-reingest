package gs.nathan.eventhubsreingest.output

import com.microsoft.azure.eventhubs.ConnectionStringBuilder
import org.apache.spark.SparkConf

case class EventHubPublisherConfig(ns: String, hub: String, keyName: String, keyValue: String, msToWaitAfterBatch: Long = 260l)

object EventHubPublisherConfig {
  def apply(sparkConf: SparkConf, prefix: String):EventHubPublisherConfig = {
    val appConf = sparkConf.getAllWithPrefix(prefix)
      .toMap
      .map(v => (v._1.stripPrefix("."), v._2))

    val msToWaitAfterBatch = appConf.getOrElse("ms_to_wait_after_batch", "260").toLong

    EventHubPublisherConfig(
      appConfig(appConf, "ns"),
      appConfig(appConf, "name"),
      appConfig(appConf, "keyName"),
      appConfig(appConf, "keyValue"),
      msToWaitAfterBatch
    )
  }

  def appConfig(conf: Map[String, String], key: String): String = {
    conf.getOrElse(key, throw new RuntimeException(s"$key not set!"))
  }
}