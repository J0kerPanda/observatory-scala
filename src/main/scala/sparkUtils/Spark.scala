package sparkUtils

import java.time.LocalDate

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, SparkSession}

trait Spark extends {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .config(new SparkConf()
      .setAppName("Observatory")
      .setMaster("local")
    )
    .getOrCreate()

}
