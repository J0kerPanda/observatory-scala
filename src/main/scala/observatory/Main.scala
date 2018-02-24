package observatory

import org.apache.log4j.{Level, Logger}

object Main extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Extraction.locateTemperatures(1950, "/stations.csv", "/2015.csv").foreach(println)
}
