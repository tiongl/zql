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

object ListTable {

  def apply[ROW: ClassTag](cols: ((ROW)=> Any)*) = {

  }

}


class ListData[ROW](val list: List[ROW]) extends RowBased[ROW] {

  implicit def listToListData[T](list: List[T]) = new ListData[T](list)

  def select(r: (ROW) => Row): RowBased[Row] = list.map(r).toList

  override def filter(filter: (ROW) => Boolean): RowBased[ROW] = list.filter(filter)

  def groupBy(rowBased: RowBased[ROW], keyFunc: (ROW) => Seq[Any], valueFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBased[Row] = {
    val data = rowBased.asInstanceOf[ListData[ROW]].list
    Utils.groupBy[ROW, Row](data, keyFunc(_), valueFunc(_), _.aggregate(_, aggregatableIndices)).map(_.normalize).toList
  }

  def reduce(reduceFunc: (ROW, ROW) => ROW) = List(list.reduce(reduceFunc))

  def map(mapFunc: (ROW) => Row) = list.map(mapFunc)

  def sortBy[K](keyFunc: (ROW) => K, ordering: Ordering[K], ctag: ClassTag[K]) = new ListData(list.sortBy(keyFunc)(ordering))

  def slice(offset: Int, until: Int) = list.slice(offset, until)

  def size() = list.length

  def asList = list

  def isLazy = false
}