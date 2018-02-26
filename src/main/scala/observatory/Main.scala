package observatory

import java.util.Calendar

import org.apache.log4j.{Level, Logger}

object Main extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val stations = Extraction.stationsDS("/stations.csv")
  val readings = Extraction.stationsReadingsDS("/2015.csv")

  println(Calendar.getInstance().getTime)
  println(Extraction.aggregateAverageTemperature(Extraction.locationReadingsDS(stations, readings, 2015)).count())

  println(Calendar.getInstance().getTime)
  println(Extraction.locationYearlyAverageRecords(Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")).count(_ => true))

  println(Calendar.getInstance().getTime)

  //  Extraction.locationYearlyAverageRecords(Extraction.locateTemperatures(1950, "/stations.csv", "/2015.csv"))
}
