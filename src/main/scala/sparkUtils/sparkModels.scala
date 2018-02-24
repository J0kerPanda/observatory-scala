package sparkUtils

import observatory.{Day, Location, Month, Temperature}

case class Station(stnId: Option[Long], wbanId: Option[Long], location: Option[Location])

case class TemperatureReading(stnId: Option[Long], wbanId: Option[Long], month: Option[Month], day: Option[Day], temperature: Option[Temperature])
