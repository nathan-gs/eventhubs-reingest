package gs.nathan.eventhubsreingest

import java.sql.Timestamp

case class Event(ts: Timestamp, body: Array[Byte], eh_partition: Int)
