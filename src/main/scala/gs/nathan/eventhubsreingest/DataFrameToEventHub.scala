package gs.nathan.eventhubsreingest


import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, rand}

import scala.util.Try


class DataFrameToEventHub(publisher: EventPublisher) extends Serializable with Logger {

  def apply(df: DataFrame, colSpec: ColSpec, orderBy: Seq[String]):Seq[Try[Unit]] = {

    val withRenamedColumns = df
      .withColumn("event", col(colSpec.event))
      .withColumn("ts", col(colSpec.ts))

    val withRandomPartition = withRenamedColumns
      .withColumn("randomPartition", (rand(0) * publisher.numberOfPartitions.get).cast("int"))

    val orderByWithPartition = (orderBy :+ "randomPartition").map(col(_))
    val ordered = withRandomPartition
      .orderBy(orderByWithPartition:_*)

    val publishEvents = ordered.rdd.mapPartitions(iter => processPartition(iter))

    publishEvents
      .collect()
      .toSeq
  }


  private def processPartition(iter: Iterator[Row]):Iterator[Try[Unit]] = {
    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }

    val items = iter
      .map(r => {
        TsPartitionEvent(r.getAs("ts"), r.getAs("randomPartition"), r.getAs[publisher.EventType]("event"))
      })
      .toSeq

    items
      .groupBy(_.partition)
      .map { case (p, seq) => {
        log.info(s"Publishing on partition $p, from ${seq.map(_.ts).min} to ${seq.map(_.ts).max}")

        publisher.send(p, seq
          .sortBy(_.ts)
          .map(_.event))
        }}
      .toIterator
  }
}

case class ColSpec(event: String = "Body", ts: String = "EnqueuedTimeUtc")
case class TsPartitionEvent(ts: Timestamp, partition: Int, event: Array[Byte])