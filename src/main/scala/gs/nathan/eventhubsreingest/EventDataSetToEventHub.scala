package gs.nathan.eventhubsreingest


import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, dayofyear, hour, rand, year}

import scala.util.Try


class EventDataSetToEventHub(publisher: EventPublisher, spark: SparkSession) extends Serializable with Logger {

  def apply(ds: Dataset[Event]):Seq[Try[Unit]] = {

    import spark.implicits._
    val withRandomPartition = ds
      .withColumn("randomPartition", (rand(0) * publisher.numberOfPartitions.get).cast("int"))
      .as[TsPartitionEvent]


    val ordered = withRandomPartition
      .orderBy(year(col("ts")), dayofyear(col("ts")), hour(col("ts")), col("randomPartition"))

    val publishEvents = ordered.rdd.mapPartitions(iter => processPartition(iter))

    publishEvents
      .collect()
      .toSeq
  }


  private def processPartition(iter: Iterator[TsPartitionEvent]):Iterator[Try[Unit]] = {
    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }

    val items = iter
      .toSeq

    items
      .groupBy(_.randomPartition)
      .map { case (p, seq) => {
        log.info(s"Publishing on partition $p, from ${seq.map(_.ts).min} to ${seq.map(_.ts).max}")

        publisher.send(p, seq
          .sortBy(_.ts)
          .map(t => Event(t.ts, t.body)))
        }}
      .toIterator
  }
}

case class TsPartitionEvent(ts: Timestamp, randomPartition: Int, body: Array[Byte])