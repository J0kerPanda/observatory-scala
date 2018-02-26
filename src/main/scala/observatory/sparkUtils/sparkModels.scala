package observatory.sparkUtils

import observatory.{Day, Location, Month, Temperature}

case class Station(id: String,
                   lat: Double,
                   lon: Double)

case class StationReading(id: String,
                          month: Month,
                          day: Day,
                          temperature: Temperature)

case class LocationReading(location: Location, epochDay: Long, temperature: Temperature)