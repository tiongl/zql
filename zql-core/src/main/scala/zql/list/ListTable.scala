package zql.list

import zql.core._
import zql.core.util.Utils
import zql.core.util.Utils.SeqOrdering

import scala.collection.mutable
import scala.reflect.ClassTag



class ListTable[ROW: ClassTag](list: List[ROW], schema: TypedSchema[ROW]) extends RowBasedTable[ROW](schema) {

  val data = new ListData(list)

  override def createTable[T: ClassTag](rowBased: RowBased[T], newSchema: TypedSchema[T]): RowBasedTable[T] = {
    val list = rowBased.asInstanceOf[ListData[T]].list
    new ListTable(list, newSchema)
  }
}


class ListData[ROW](val list: List[ROW]) extends RowBased[ROW] {
  def select(r: (ROW) => Row): RowBased[Row] = {
    new ListData(list.map(r).toList)
  }

  override def filter(filter: (ROW) => Boolean): RowBased[ROW] = {
    val filtered = list.filter(filter)
    new ListData(filtered)
  }

  def groupBy(rowBased: RowBased[ROW], keyFunc: (ROW) => Seq[Any], valueFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBased[Row] = {
    val data = rowBased.asInstanceOf[ListData[ROW]].list
    val groupedData = Utils.groupBy[ROW, Row](data,
      (row: ROW) => keyFunc(row),
      (row: ROW) => valueFunc(row),
      (a: Row, b: Row) => a.aggregate(b, aggregatableIndices)
    ).map(_.normalize).toList
    new ListData[Row](groupedData)
  }

  def reduce(reduceFunc: (ROW, ROW) => ROW) = new ListData(List(list.reduce(reduceFunc)))

  def map(mapFunc: (ROW) => Row) = new ListData(list.map(mapFunc))

  def sortBy[K](keyFunc: (ROW) => K, ordering: Ordering[K], ctag: ClassTag[K]): RowBased[ROW] = new ListData(list.sortBy(keyFunc)(ordering))

  def slice(offset: Int, until: Int) = new ListData(list.slice(offset, until))

  def size() = list.length

  def asList = list
}