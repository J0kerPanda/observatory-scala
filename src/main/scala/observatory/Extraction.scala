package observatory

import java.time.LocalDate

import org.apache.spark.sql.Row
import sparkUtils.{Spark, Station, TemperatureReading}

/**
  * 1st milestone: data extraction
  */
object Extraction extends Spark {

  import sparkSession.implicits._

  private def idConverter: PartialFunction[String, Long] = {
    case str if str.nonEmpty => str.toLong
  }

  private def intConverter: PartialFunction[String, Int] = {
    case str if str.nonEmpty => str.toInt
  }

  private def doubleConverter: PartialFunction[String, Double] = {
    case str if str.nonEmpty => str.toDouble
  }

  private val temperatureExclusion: Double = 9999.9

  private def temperatureConverter: PartialFunction[String, Double] = {
    case str if str.nonEmpty => (str.toDouble - 32) * 5 / 9
  }

  private def getRowValue[T](i: Int, converter: PartialFunction[String, T])(implicit row: Row): Option[T] = {
    Option(row.getString(i)).collect(converter)
  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    val stationsDS = sparkSession.read.csv(this.getClass.getResource(stationsFile).getPath).map { row =>
      implicit val _r: Row = row
      Station(
        stnId = getRowValue(0, idConverter),
        wbanId = getRowValue(1, idConverter),
        location = for {
          lat <- getRowValue(2, doubleConverter)
          lon <- getRowValue(3, doubleConverter)
        } yield Location(lat, lon)
      )
    }.filter(_.location.nonEmpty)

    val readingsDS = sparkSession.read.csv(this.getClass.getResource(temperaturesFile).getPath).map { row =>
      implicit val _r: Row = row
      TemperatureReading(
        stnId = getRowValue(0, idConverter),
        wbanId = getRowValue(1, idConverter),
        month = getRowValue(2, intConverter),
        day = getRowValue(3, intConverter),
        temperature = getRowValue(4, temperatureConverter)
      )
    }



    readingsDS.show(100)



    List((LocalDate.MAX, Location(0, 0), 1.0))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

}
