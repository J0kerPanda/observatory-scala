package sparkUtils

import java.time.LocalDate

import observatory.{Location, Temperature}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

trait Spark extends {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(new SparkConf()
      .setAppName("Observatory")
      .setMaster("local")
      .registerKryoClasses(Array(classOf[LocalDate]))
    )
    .getOrCreate()

  implicit val localDateEncoder: Encoder[LocalDate] = Encoders.kryo[LocalDate]
  implicit val localReadingEncoder: Encoder[(LocalDate, Location, Temperature)] = Encoders.kryo[(LocalDate, Location, Temperature)]
  implicit val locationReadingEncoder: Encoder[LocationReading] = Encoders.kryo[LocationReading]
}
