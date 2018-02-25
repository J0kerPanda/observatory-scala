package sparkUtils

import java.time.LocalDate

import observatory.{Day, Location, Month, Temperature}

case class Station(stnId: Option[Long], wbanId: Option[Long], location: Option[Location])

case class StationReading(stnId: Option[Long], wbanId: Option[Long], month: Option[Month], day: Option[Day], temperature: Option[Temperature])

case class LocationReading(date: LocalDate, location: Location, temperature: Temperature)
