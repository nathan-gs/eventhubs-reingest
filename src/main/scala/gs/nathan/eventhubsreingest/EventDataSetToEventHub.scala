package gs.nathan.eventhubsreingest


import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, dayofyear, hour, rand, year}

import scala.util.{Success, Try}


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

    iter
      .toSeq
      .groupBy(_.randomPartition)
      .map { case (p, seq) => {
        val sorted = seq.sortBy(_.ts)
        sorted.headOption match {
          case Some(head) => {
            log.info(s"Publishing on partition $p, from ${head.ts} to ${sorted.last.ts}, number of msgs: ${sorted.size}")

            publisher.send(p, sorted
              .map(t => Event(t.ts, t.body)))
          }
          case None => {
            log.warn(s"Empty iterator for partition $p")
            Success[Unit]()
          }
        }
      }
    }
    .toIterator
  }
}

case class TsPartitionEvent(ts: Timestamp, randomPartition: Int, body: Array[Byte])