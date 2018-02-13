package gs.nathan.eventhubsreingest

import java.sql.Timestamp
import java.time.temporal.TemporalAmount

object TimestampRange {

  def apply(start: Timestamp, end: Timestamp, step: TemporalAmount): Seq[(Timestamp, Timestamp)] = {
    Iterator
      .iterate(start.toInstant)(_.plus(step))
      .takeWhile(!_.isAfter(end.toInstant.plus(step)))
      .sliding(2)
      .map(t => (new Timestamp(t.head.toEpochMilli), new Timestamp(t.tail.head.toEpochMilli)))
      .toSeq
  }

}
