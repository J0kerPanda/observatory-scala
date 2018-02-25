package sparkUtils

import org.apache.spark.sql.Dataset

case class DatasetIterable[T](ds: Dataset[T]) extends Iterable[T] {

  import scala.collection.JavaConverters._
  override def iterator: Iterator[T] = ds.toLocalIterator().asScala
}
