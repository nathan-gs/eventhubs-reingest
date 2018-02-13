package gs.nathan.eventhubsreingest


import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, max, min}

import scala.util.{Failure, Success}



class EventDataSetToEventPublisher(publisher: EventPublisher, spark: SparkSession) extends Serializable with Logger {

  val TsColumn = "ts"
  val PartitionColumn = "eh_partition"

  def apply(ds: Dataset[Event]):Seq[PublishResult] = {
    val numberOfPartitions = publisher.numberOfPartitions.get

    val f = ds.agg(min(TsColumn) as "ts_min", max(TsColumn)as "ts_max").first()
    val minTs = f.getAs[Timestamp]("ts_min")
    val maxTs = f.getAs[Timestamp]("ts_max")

    log.info(s"Processing everything between $minTs till $maxTs")

    val range = TimestampRange(minTs, maxTs, Duration.ofHours(2))

    import spark.implicits._

    val publishEvents = range.flatMap { case (start, end) =>
      log.info(s"Processing $start till $end")

      val hourly = ds.where(col(TsColumn) >= start).where(col(TsColumn) < end)

      val results =  hourly
        .repartition(numberOfPartitions, col(PartitionColumn))
        //.sortWithinPartitions(col(PartitionColumn), col(TsColumn))
        .mapPartitions(processPartition(_))
        .collect()


      log.info(s"Processing finished for $start till $end")
      results
    }

    publishEvents
  }





  private def processPartition(iter: Iterator[Event]):Iterator[PublishResult] = {
    implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }



    iter
      .toSeq
      .groupBy(_.eh_partition)
      .map { case (p, seq) =>
        val sorted = seq.sortBy(_.ts)
        sorted.headOption match {
          case Some(head) =>
            log.info(s"Publishing on partition $p, from ${head.ts} to ${sorted.last.ts}, number of msgs: ${sorted.size}")

            publisher.send(p, sorted) match {
              case Success(stats) => PublishResult(p, Some(stats), None, None)
              case Failure(e) => PublishResult(p, None, Some(e.getClass.getSimpleName), Some(e.getMessage))
            }
          case None =>
            log.warn(s"Empty iterator for partition $p")
            PublishResult(p, None, Some(""), Some("Empty iterator"))
        }
      }
    .toIterator
  }

}
