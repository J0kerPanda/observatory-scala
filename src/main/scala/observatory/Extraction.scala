package observatory

import java.time.LocalDate

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}
import sparkUtils._


/**
  * 1st milestone: data extraction
  */
object Extraction extends Spark {

  import sparkSession.implicits._

  private def temperatureConverter: PartialFunction[Double, Double] = {
    case t if t != 9999.9 => (t.toDouble - 32) * 5 / 9
  }

  private def getRowValue[T](i: Int)(implicit row: Row): Option[T] = {
    if (row.isNullAt(i)) {
      None
    } else {
      Some(row.getAs[T](i))
    }
  }

  private def getRowValue[T, U](i: Int, converter: PartialFunction[T, U])(implicit row: Row): Option[U] = {
    getRowValue(i).collect(converter)
  }

  //todo think about frameless
  def stationsDS(stationsFile: String): Dataset[Station] = {

    val schema = StructType(List(
      StructField("stnId", StringType, nullable = true),
      StructField("wbanId", StringType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("lon", DoubleType, nullable = true)
    ))

    sparkSession.read.schema(schema).csv(this.getClass.getResource(stationsFile).getPath)
    .map { row =>
      implicit val _r: Row = row
      Station(
        stnId = getRowValue(0),
        wbanId = getRowValue(1),
        location = for {
          lat <- getRowValue[Double](2)
          lon <- getRowValue[Double](3)
        } yield Location(lat, lon)
      )
    }
    .filter {
      s: Station => s.valid
    }
  }

  def stationsReadingsDS(temperaturesFile: String): Dataset[StationReading] = {

    val schema = StructType(List(
      StructField("stnId", StringType, nullable = true),
      StructField("wbanId", StringType, nullable = true),
      StructField("month", IntegerType, nullable = true),
      StructField("day", IntegerType, nullable = true),
      StructField("temperature", DoubleType, nullable = true)
    ))

    sparkSession.read.schema(schema).csv(this.getClass.getResource(temperaturesFile).getPath).map { row =>
      implicit val _r: Row = row
      StationReading(
        stnId = getRowValue(0),
        wbanId = getRowValue(1),
        month = getRowValue(2),
        day = getRowValue(3),
        temperature = getRowValue(4, temperatureConverter)
      )
    }
    .filter {
      r: StationReading => r.valid
    }
  }

  def locationReadingsDS(st: Dataset[Station], rd: Dataset[StationReading], year: Year): Dataset[LocationReading] = {
    val stations = st.repartition($"stnId", $"wbanId")
    val readings = rd.repartition($"stnId", $"wbanId")

    readings.joinWith(stations,
      (stations("stnId") === readings("stnId")) &&
      (stations("wbanId") === readings("wbanId"))
    )
    .map { case (reading, station) =>
      LocationReading(
        location = station.location.get,
        epochDay = LocalDate.of(year, reading.month.get, reading.day.get).toEpochDay,
        temperature = reading.temperature.get
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

    val stations = stationsDS(stationsFile)
    val readings = stationsReadingsDS(temperaturesFile)
    val locationReadings = locationReadingsDS(stations, readings, year)

    import scala.collection.JavaConverters._
    locationReadings.toLocalIterator().asScala.toStream.map(lr => (LocalDate.ofEpochDay(lr.epochDay), lr.location, lr.temperature))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {

    val transformed = records.toStream.map {
      case (date, l, t) => LocationReading(l, date.toEpochDay, t)
    }

    val readings = sparkSession.sparkContext.parallelize(transformed).toDS()

    import scala.collection.JavaConverters._
    aggregateAverageTemperature(readings).toLocalIterator().asScala.toStream
  }
}
