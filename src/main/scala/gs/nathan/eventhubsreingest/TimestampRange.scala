package gs.nathan.eventhubsreingest

import java.sql.Timestamp
import java.time.temporal.TemporalAmount

object TimestampRange {

  def apply(pair: TimestampPair, step: TemporalAmount): Seq[TimestampPair] = {
    Iterator
      .iterate(pair.start.toInstant)(_.plus(step))
      .takeWhile(!_.isAfter(pair.end.toInstant.plus(step)))
      .sliding(2)
      .map(t => TimestampPair(new Timestamp(t.head.toEpochMilli), new Timestamp(t.tail.head.toEpochMilli)))
      .toSeq
  }

}

case class TimestampPair(start: Timestamp, end: Timestamp)