package observatory

import java.time.LocalDate

import observatory.sparkUtils._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.concat_ws


/**
  * 1st milestone: data extraction
  */
object Extraction extends Spark {

  import sparkSession.implicits._

  //todo think about frameless
  def stationsDS(stationsFile: String): Dataset[Station] = {

    val schema = StructType(List(
      StructField("stnId", StringType, nullable = true),
      StructField("wbanId", StringType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("lon", DoubleType, nullable = true)
    ))

    sparkSession.read.schema(schema).csv(this.getClass.getResource(stationsFile).getPath)
      .withColumn("id", concat_ws("_", $"stnId", $"wbanId"))
      .drop("stnId", "wbanId")
      .na.drop()
      .as[Station]
  }

  def stationsReadingsDS(temperaturesFile: String): Dataset[StationReading] = {

    val schema = StructType(List(
      StructField("stnId", StringType, nullable = true),
      StructField("wbanId", StringType, nullable = true),
      StructField("month", IntegerType, nullable = true),
      StructField("day", IntegerType, nullable = true),
      StructField("temperatureF", DoubleType, nullable = true)
    ))

    sparkSession.read.schema(schema).csv(this.getClass.getResource(temperaturesFile).getPath)
      .withColumn("id", concat_ws("_", $"stnId", $"wbanId"))
      .where($"temperatureF" < 9999.9)
      .withColumn("temperature", ($"temperatureF" - 32) * 5 / 9)
      .drop("stnId", "wbanId", "temperatureF")
      .na.drop()
      .as[StationReading]
  }

  def locationReadingsDS(stations: Dataset[Station], readings: Dataset[StationReading], year: Year): Dataset[LocationReading] = {
    readings.joinWith(stations, stations("id") === readings("id"))
      .map { case (reading, station) =>
        LocationReading(
          location = Location(station.lat, station.lon),
          epochDay = LocalDate.of(year, reading.month, reading.day).toEpochDay,
          temperature = reading.temperature
        )
      }
  }

  def aggregateAverageTemperature(locationReadings: Dataset[LocationReading]): Dataset[(Location, Temperature)] = {
    locationReadings
      .groupBy($"location")
      .avg("temperature")
      .map { row =>
        val location = row.getStruct(0)
        (Location(location.getDouble(0), location.getDouble(1)), row.getDouble(1))
      }
  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {

    locationReadingsDS(stationsDS(stationsFile), stationsReadingsDS(temperaturesFile), year)
      .collect()
      .par
      .map(lr => (LocalDate.ofEpochDay(lr.epochDay), lr.location, lr.temperature))
      .seq
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records.par.groupBy(_._2)
      .mapValues {
        values => values.aggregate(0.0)(
          {
            case (acc, (_, _, temperature)) => acc + temperature
          },
          _ + _
        ) / values.size

      }
    .seq
  }
}
