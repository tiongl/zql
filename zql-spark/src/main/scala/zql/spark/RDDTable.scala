package zql.spark

import org.apache.spark.rdd.RDD
import zql.core._
import zql.core.util.Utils.SeqOrdering
import zql.list.ListTable

import scala.reflect.ClassTag

class RDDData[T](val rdd: RDD[T]) extends RowBased[T]{
  override def slice(offset: Int, untilN: Int): RowBased[T] = {
    val newRdd = rdd.zipWithIndex().filter(t => t._2 >= offset && t._2 < untilN).map[Row](_._1.asInstanceOf[Row])
    new RDDData(newRdd).asInstanceOf[RDDData[T]]
  }

  override def reduce(reduceFunc: (T, T) => T): RowBased[T] = {
    val reduce = rdd.reduce(reduceFunc)
    new RDDData(rdd.sparkContext.parallelize[Row](Seq(reduce.asInstanceOf[Row]), 1)).asInstanceOf[RDDData[T]]
  }

  override def filter(filter: (T) => Boolean): RowBased[T] = new RDDData(rdd.filter(filter))

  override def groupBy(rowBased: RowBased[T], keyFunc: (T) => Seq[Any], valueFunc: (T) => Row, aggregatableIndices: Array[Int]): RowBased[Row] = {
    new RDDData(rdd.map{ row => ( keyFunc(row), valueFunc(row))}.reduceByKey(_.aggregate(_, aggregatableIndices)).map(_._2))
  }

  override def size: Int = ???

  override def select(r: (T) => Row): RowBased[Row] = new RDDData(rdd.map(r))

  override def sortBy[K](keyFunc: (T) => K, ordering: Ordering[K], classTag: ClassTag[K]): RowBased[T] = {
    new RDDData(rdd.sortBy(keyFunc)(ordering, classTag))
  }

  override def asList: List[T] = rdd.collect().toList

  override def map(mapFunc: (T) => Row): RowBased[Row] = new RDDData(rdd.map(mapFunc))

  def isLazy = true
}

class RDDTable[ROW](rdd: RDD[ROW], schema: TypedSchema[ROW]) extends RowBasedTable(schema) {
  override def data: RowBased[ROW] = new RDDData(rdd)

  override def createTable[T: ClassManifest](rowBased: RowBased[T], newSchema: TypedSchema[T]): RowBasedTable[T] = {
    val rdd = rowBased.asInstanceOf[RDDData[T]].rdd
    new RDDTable[T](rdd, newSchema)
  }
}