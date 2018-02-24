package observatory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val sc = new SparkContext("local", "Observatory", new SparkConf())

  sc.textFile("src/main/resources/test.txt")
    .collect()
    .toList
    .foreach(println)

}
