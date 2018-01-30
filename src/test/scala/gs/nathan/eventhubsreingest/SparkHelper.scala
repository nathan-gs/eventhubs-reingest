package gs.nathan.eventhubsreingest

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, WordSpec}

trait SparkHelper extends WordSpec with BeforeAndAfter {


  before {
    sparkSession()
  }

  after {
    sparkSession().stop()
    iSpark = null
  }

  private var iSpark: SparkSession = null

  def sparkSession(): SparkSession = {
    if(iSpark == null) {
      createNewSparkSession()
    }
    iSpark
  }

  def createNewSparkSession():SparkSession = {
    if(iSpark != null) {
      iSpark.stop()
    }
    iSpark = SparkSession
      .builder()
      .master("local[2]")
      .appName(getClass.getSimpleName)
      .getOrCreate()


    iSpark
  }
}
