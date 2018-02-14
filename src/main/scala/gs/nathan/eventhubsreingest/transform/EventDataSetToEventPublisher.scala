package gs.nathan.eventhubsreingest.transform

import java.sql.Timestamp
import java.time.Duration
import java.util.{Locale, TimeZone}

import gs.nathan.eventhubsreingest._
import gs.nathan.eventhubsreingest.output.EventPublisher
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success}



class EventDataSetToEventPublisher(publisher: EventPublisher, spark: SparkSession, config: EventDataSetToEventPublisherConfig) extends Serializable with Logger {

  val TsColumn = "ts"
  val PartitionColumn = "eh_partition"

  def apply(ds: Dataset[Event]):Seq[PublishResult] = {
    val numberOfPartitions = publisher.numberOfPartitions.get

    val startEnd = config.timestampPair.getOrElse({
      val f = ds.agg(min(TsColumn) as "ts_min", max(TsColumn)as "ts_max").first()
      val minTs = f.getAs[Timestamp]("ts_min")
      val maxTs = f.getAs[Timestamp]("ts_max")
      TimestampPair(minTs, maxTs)
    })


    log.info(s"Processing everything between ${startEnd.start} till ${startEnd.end}")

    val range = TimestampRange(startEnd, config.timeStep)

    import spark.implicits._

    val publishEvents = range.flatMap { case TimestampPair(start, end) =>
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

case class EventDataSetToEventPublisherConfig(timeStep: Duration, timestampPair: Option[TimestampPair])
object EventDataSetToEventPublisherConfig {
  def apply(sparkConf: SparkConf, prefix: String):EventDataSetToEventPublisherConfig = {
    val appConf = sparkConf.getAllWithPrefix(prefix)
      .toMap
      .map(v => (v._1.stripPrefix("."), v._2))

    val timeStep = Duration.ofHours(appConf.getOrElse("hours_to_process_in_step", "24").toLong)
    val pair = if (appConf.isDefinedAt("startDate") && appConf.isDefinedAt("endDate")) {
      Some(TimestampPair(
        stringToTs(appConf.getOrElse("startDate", throw new RuntimeException("startDate not set"))),
        stringToTs(appConf.getOrElse("endDate", throw new RuntimeException("endDate not set")))
      ))
    } else {
      None
    }


    EventDataSetToEventPublisherConfig(
      timeStep,
      pair
    )
  }

  def stringToTs(s: String): Timestamp = {
    import java.sql.Timestamp
    import java.text.SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT)

    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val parsedDate = dateFormat.parse(s)
    val timestamp = new Timestamp(parsedDate.getTime)

    timestamp
  }

}