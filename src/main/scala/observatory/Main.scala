package observatory

import org.apache.log4j.{Level, Logger}

object Main extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val stations = Extraction.stationsDS("/stations.csv").cache()
  val readings = Extraction.stationsReadingsDS("/2015.csv")

  Extraction.aggregateAverageTemperature(stations, Extraction.locationReadingsDS(stations, readings, 2015))

//  Extraction.locationYearlyAverageRecords(Extraction.locateTemperatures(1950, "/stations.csv", "/2015.csv"))
}
