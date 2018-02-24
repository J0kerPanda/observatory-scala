package observatory

import java.time.LocalDate

import spark.Spark

/**
  * 1st milestone: data extraction
  */
object Extraction extends Spark {


  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    sparkContext.textFile(getClass.getResource(stationsFile).getPath)
      .collect()
      .toList
      .foreach(println)

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
