package zql.spark

import org.apache.spark.rdd.RDD
import zql.core
import zql.core._
import zql.rowbased.{ Row, RowBasedTable, RowBasedData }
import zql.schema.Schema
import scala.reflect.ClassTag

class RDDData[R: ClassTag](val rdd: RDD[R], val option: CompileOption = new CompileOption()) extends RowBasedData[R] {

  implicit def rddToRDDData[T: ClassTag](rdd: RDD[T]) = new RDDData(rdd, option)

  //This is slow as we zipWithIndex to filter the rows we need.
  override def slice(offset: Int, untilN: Int) = rdd.zipWithIndex().filter(t => t._2 >= offset && t._2 < untilN).map(_._1)

  override def reduce(reduceFunc: (R, R) => R): RowBasedData[R] = {
    val reduce = rdd.reduce(reduceFunc)
    rdd.sparkContext.parallelize[R](Seq(reduce), 1)
  }

  override def filter(filter: (R) => Boolean): RowBasedData[R] = rdd.filter(filter)

  override def groupBy(keyFunc: (R) => Row, valueFunc: (R) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row] = {
    val tuples = rdd.map { row => (keyFunc(row), valueFunc(row)) }
    tuples.reduceByKey(_.aggregate(_, aggregatableIndices)).map(_._2)
  }

  override def size: Int = ???

  override def sortBy(keyFunc: (R) => Row, ordering: Ordering[Row], classTag: ClassTag[Row]): RowBasedData[R] = {
    rdd.sortBy(keyFunc)(ordering, classTag)
  }

  override def asList[T]: List[T] = rdd.collect().toList.asInstanceOf[List[T]]

  override def map[T: ClassTag](mapFunc: (R) => T): RowBasedData[T] = rdd.map(mapFunc)

  override def isLazy = true

  override def withOption(option: CompileOption): RowBasedData[R] = new RDDData[R](rdd, option)

  override def distinct(): RowBasedData[R] = rdd.distinct()

  override def join(other: RowBasedData[Row], jointPoint: (Row) => Boolean): RowBasedData[Row] = other match {
    case rddData: RDDData[Row] =>
      rdd.asInstanceOf[RDD[Row]].cartesian[Row](rddData.rdd).map {
        case (r1, r2) => new Row(r1.data ++ r2.data)
      }.filter(jointPoint.apply(_))
    case _ =>
      throw new IllegalArgumentException("Unsupported join type " + other.toString)
  }
}

class RDDTable[ROW: ClassTag](schema: Schema, rdd: RDD[ROW]) extends RowBasedTable[ROW](schema) {

  override def data: RowBasedData[ROW] = new RDDData(rdd)

  override def createTable[T: ClassManifest](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T] = {
    val rdd = rowBased.asInstanceOf[RDDData[T]].rdd
    new RDDTable[T](newSchema, rdd)
  }
}

object RDDTable {
}