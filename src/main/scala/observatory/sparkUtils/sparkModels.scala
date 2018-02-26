package observatory.sparkUtils

import observatory.{Day, Location, Month, Temperature}

case class Station(stnId: String,
                   wbanId: String,
                   lat: Double,
                   lon: Double)

case class StationReading(stnId: String,
                          wbanId: String,
                          month: Month,
                          day: Day,
                          temperature: Temperature)

case class LocationReading(location: Location, epochDay: Long, temperature: Temperature)

case class TemperatureAgg(stnId: Option[Long], wbanId: Option[Long], average: Double)