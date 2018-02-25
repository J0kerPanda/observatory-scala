package observatory.sparkUtils

import observatory.{Day, Location, Month, Temperature}

case class Station(stnId: Option[String],
                   wbanId: Option[String],
                   location: Option[Location]) {

  def valid: Boolean = (stnId.exists(_.nonEmpty) || wbanId.exists(_.nonEmpty)) && location.nonEmpty
}

case class StationReading(stnId: Option[String],
                          wbanId: Option[String],
                          month: Option[Month],
                          day: Option[Day],
                          temperature: Option[Temperature]) {

  def valid: Boolean = (stnId.exists(_.nonEmpty) || wbanId.exists(_.nonEmpty)) && month.nonEmpty && day.nonEmpty && temperature.nonEmpty
}

case class LocationReading(location: Location, epochDay: Long, temperature: Temperature)

case class TemperatureAgg(stnId: Option[Long], wbanId: Option[Long], average: Double)