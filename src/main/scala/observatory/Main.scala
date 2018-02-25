package observatory

import org.apache.log4j.{Level, Logger}

object Main extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  Extraction.aggregateAverageTemperature(Extraction.locationReadingsDS(2015, "/stations.csv", "/2015.csv"))

//  Extraction.locationYearlyAverageRecords(Extraction.locateTemperatures(1950, "/stations.csv", "/2015.csv"))
}
