package gs.nathan.eventhubsreingest.input

import gs.nathan.eventhubsreingest.Event
import org.apache.spark.sql.Dataset

import scala.util.Try

trait ProduceEventDataSet {

  def apply(): Try[Dataset[Event]]

}
