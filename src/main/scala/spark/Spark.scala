package spark

import org.apache.spark.{SparkConf, SparkContext}

trait Spark {

  val sparkContext = new SparkContext("local", "Observatory", new SparkConf())
}
