package sparkUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Spark extends {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("Observatory")
    .config(new SparkConf().setMaster("local"))
    .getOrCreate()
}
