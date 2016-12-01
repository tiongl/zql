package zql.spark

import org.apache.spark.rdd.RDD
import zql.core._
import zql.core.util.Utils.SeqOrdering
import zql.list.ListTable

import scala.reflect.ClassTag

class RDDData[T: ClassTag](val rdd: RDD[T]) extends RowBased[T]{

  implicit def rddToRDDData[T: ClassTag](rdd: RDD[T]) = new RDDData(rdd)

  //This is slow as we zipWithIndex to filter the rows we need.
  override def slice(offset: Int, untilN: Int) = rdd.zipWithIndex().filter(t => t._2 >= offset && t._2 < untilN).map(_._1)



  override def reduce(reduceFunc: (T, T) => T): RowBased[T] = {
    val reduce = rdd.reduce(reduceFunc)
    rdd.sparkContext.parallelize[T](Seq(reduce), 1)
  }

  override def filter(filter: (T) => Boolean): RowBased[T] = rdd.filter(filter)

  override def groupBy(rowBased: RowBased[T], keyFunc: (T) => Seq[Any], valueFunc: (T) => Row, aggregatableIndices: Array[Int]): RowBased[Row] = {
    rdd.map{ row => ( keyFunc(row), valueFunc(row))}.
      reduceByKey(_.aggregate(_, aggregatableIndices)).map(_._2)
  }

  override def size: Int = ???

  override def select(r: (T) => Row): RowBased[Row] = rdd.map(r)

  override def sortBy[K](keyFunc: (T) => K, ordering: Ordering[K], classTag: ClassTag[K]): RowBased[T] = {
    rdd.sortBy(keyFunc)(ordering, classTag)
  }

  override def asList: List[T] = rdd.collect().toList

  override def map(mapFunc: (T) => Row): RowBased[Row] = rdd.map(mapFunc)

  def isLazy = true
}

class RDDTable[ROW: ClassTag](schema: TypedSchema[ROW], rdd: RDD[ROW]) extends RowBasedTable(schema) {
  override def data: RowBased[ROW] = new RDDData(rdd)

  override def createTable[T: ClassManifest](rowBased: RowBased[T], newSchema: TypedSchema[T]): RowBasedTable[T] = {
    val rdd = rowBased.asInstanceOf[RDDData[T]].rdd
    new RDDTable[T](newSchema, rdd)
  }
}


object RDDTable {
  def apply[ROW: ClassTag](cols: TypedColumnDef[ROW]*)(data: RDD[ROW]) = {
    val schema = new TypedSchema[ROW](cols: _*)
    new RDDTable[ROW](schema, data)
  }
}