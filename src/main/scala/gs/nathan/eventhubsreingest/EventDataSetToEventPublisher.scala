package gs.nathan.eventhubsreingest


import java.sql.Timestamp

import org.apache.spark.sql.Dataset

import scala.util.{Success, Try}


class EventDataSetToEventPublisher(publisher: EventPublisher) extends Serializable with Logger {

  def apply(ds: Dataset[Event]):Seq[Try[Unit]] = {
    val publishEvents = ds.rdd.mapPartitions(iter => processPartition(iter))

    publishEvents
      .collect()
      .toSeq
  }


  private def processPartition(iter: Iterator[Event]):Iterator[Try[Unit]] = {
    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }

    iter
      .toSeq
      .groupBy(_.eh_partition)
      .map { case (p, seq) => {
        val sorted = seq.sortBy(_.ts)
        sorted.headOption match {
          case Some(head) => {
            log.info(s"Publishing on partition $p, from ${head.ts} to ${sorted.last.ts}, number of msgs: ${sorted.size}")

            publisher.send(p, sorted)
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
