package observatory.sparkUtils

import observatory.{Day, Location, Month, Temperature}

case class Station(stnId: Option[Long], wbanId: Option[Long], location: Option[Location]) {

  def valid: Boolean = (stnId.nonEmpty || wbanId.nonEmpty) && location.nonEmpty
}

case class StationReading(stnId: Option[Long], wbanId: Option[Long], month: Option[Month], day: Option[Day], temperature: Option[Temperature]) {

  def valid: Boolean = (stnId.nonEmpty || wbanId.nonEmpty) && month.nonEmpty && day.nonEmpty && temperature.nonEmpty
}

case class LocationReading(location: Location, epochDay: Long, temperature: Temperature)

case class TemperatureAgg(stnId: Option[Long], wbanId: Option[Long], average: Double)