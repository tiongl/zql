package zql.spark

import org.apache.spark.rdd.RDD
import zql.core
import zql.core._

import scala.reflect.ClassTag

class RDDData[T: ClassTag](val rdd: RDD[T], val option: CompileOption = new CompileOption()) extends RowBasedData[T] {

  implicit def rddToRDDData[T: ClassTag](rdd: RDD[T]) = new RDDData(rdd, option)

  //This is slow as we zipWithIndex to filter the rows we need.
  override def slice(offset: Int, untilN: Int) = rdd.zipWithIndex().filter(t => t._2 >= offset && t._2 < untilN).map(_._1)

  override def reduce(reduceFunc: (T, T) => T): RowBasedData[T] = {
    val reduce = rdd.reduce(reduceFunc)
    rdd.sparkContext.parallelize[T](Seq(reduce), 1)
  }

  override def filter(filter: (T) => Boolean): RowBasedData[T] = rdd.filter(filter)

  override def groupBy(keyFunc: (T) => Seq[Any], valueFunc: (T) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row] = {
    val tuples = rdd.map { row => (keyFunc(row), valueFunc(row)) }
    tuples.reduceByKey(_.aggregate(_, aggregatableIndices)).map(_._2)
  }

  override def size: Int = ???

  override def select(r: (T) => Row): RowBasedData[Row] = rdd.map(r)

  override def sortBy[K](keyFunc: (T) => K, ordering: Ordering[K], classTag: ClassTag[K]): RowBasedData[T] = {
    rdd.sortBy(keyFunc)(ordering, classTag)
  }

  override def asList: List[T] = rdd.collect().toList

  override def map(mapFunc: (T) => Row): RowBasedData[Row] = rdd.map(mapFunc)

  override def isLazy = true

  override def withOption(option: CompileOption): RowBasedData[T] = new RDDData[T](rdd, option)

  override def distinct(): RowBasedData[T] = rdd.distinct()
}

class RDDTable[ROW: ClassTag](schema: DefaultSchema, rdd: RDD[ROW]) extends RowBasedTable[ROW](schema) {

  val name = getClass.getCanonicalName + "-" + rdd
  override def data: RowBasedData[ROW] = new RDDData(rdd)

  override def createTable[T: ClassManifest](rowBased: RowBasedData[T], newSchema: DefaultSchema): RowBasedTable[T] = {
    val rdd = rowBased.asInstanceOf[RDDData[T]].rdd
    new RDDTable[T](newSchema, rdd)
  }
}

object RDDTable {
  def apply[ROW: ClassTag](cols: TypedColumnDef[ROW]*)(data: RDD[ROW]) = {
    val schema = new DefaultSchema(cols: _*)
    new RDDTable[ROW](schema, data)
  }
}