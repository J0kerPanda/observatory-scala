package sparkUtils

import java.time.LocalDate

import observatory.{Day, Location, Month, Temperature}

case class Station(stnId: Option[Long], wbanId: Option[Long], location: Option[Location])

case class StationReading(stnId: Option[Long], wbanId: Option[Long], month: Option[Month], day: Option[Day], temperature: Option[Temperature])

case class LocationReading(stnId: Option[Long], wbanId: Option[Long], epochDay: Long, location: Location, temperature: Temperature)

case class TemperatureAgg(stnId: Option[Long], wbanId: Option[Long], average: Double)
