package observatory.sparkUtils

import java.time.LocalDate

import observatory.{Location, Temperature}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

trait Spark extends {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(new SparkConf()
      .setAppName("Observatory")
      .setMaster("local")
      .registerKryoClasses(Array(classOf[LocalDate], classOf[LocationReading]))
    )
    .getOrCreate()

  implicit val localDateEncoder: Encoder[LocalDate] = Encoders.kryo[LocalDate]
  implicit val ld: Encoder[(LocalDate, Temperature)] = Encoders.kryo[(LocalDate, Temperature)]
  implicit val localReadingEncoder: Encoder[(LocalDate, Location, Temperature)] = Encoders.kryo[(LocalDate, Location, Temperature)]
}
