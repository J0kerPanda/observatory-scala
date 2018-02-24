package observatory

import java.time.LocalDate

import sparkUtils.Spark

/**
  * 1st milestone: data extraction
  */
object Extraction extends Spark {

  import sparkSession.implicits._

  private def idConverter: PartialFunction[String, Long] = {
    case str if str.nonEmpty => str.toLong
  }

  private def coordConverter: PartialFunction[String, Double] = {
    case str if str.nonEmpty => str.toDouble
  }

  private val temperatureExclusion: Double = 9999.9

  private def temperatureConverter: PartialFunction[String, Double] = {
    case str if str.nonEmpty => (str.toDouble - 32) * 5 / 9
  }




  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    val stationsDS = sparkSession.read.csv(this.getClass.getResource(stationsFile).getPath).map { row =>
      Station(
        stnId = Option(row.getString(0)).collect(idConverter),
        wbanId = Option(row.getString(1)).collect(idConverter),
        location = for {
          lat <- Option(row.getString(2)).collect(coordConverter)
          lon <- Option(row.getString(3)).collect(coordConverter)
        } yield Location(lat, lon)
      )
    }

    a.show()

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
