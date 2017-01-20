package zql.list

import zql.core._
import zql.rowbased.{ Row, RowBasedData, RowBasedTable }
import zql.schema.Schema
import zql.util.Utils

import scala.reflect.ClassTag

class ListTable[ROW: ClassTag](schema: Schema, list: List[ROW]) extends RowBasedTable[ROW](schema) {

  val data = new ListData(list)

  override def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): ListTable[T] = {
    val list = rowBased.asInstanceOf[ListData[T]].list
    new ListTable(newSchema, list)
  }

  override def as(alias: Symbol): Table = new ListTable[ROW](schema.as(alias), list)
}

class ListData[ROW: ClassTag](val list: List[ROW], val option: CompileOption = new CompileOption) extends RowBasedData[ROW] {

  implicit def listToListData[T: ClassTag](list: List[T]) = new ListData[T](list, option)

  override def filter(filter: (ROW) => Boolean): RowBasedData[ROW] = list.filter(filter)

  override def groupBy(keyFunc: (ROW) => Row, valueFunc: (ROW) => Row, aggregateFunc: (Row, Row) => Row): RowBasedData[Row] = {
    Utils.groupBy[ROW, Row](list, keyFunc(_), valueFunc(_), aggregateFunc).toList
  }

  override def reduce(reduceFunc: (ROW, ROW) => ROW) = List(list.reduce(reduceFunc))

  override def map[T: ClassTag](mapFunc: (ROW) => T) = list.map(mapFunc)

  override def sortBy[T: ClassTag](keyFunc: (ROW) => T, ordering: Ordering[T]) = new ListData(list.sortBy(keyFunc)(ordering))

  override def slice(offset: Int, until: Int) = list.slice(offset, until)

  override def size() = list.length

  override def asList[T] = list.asInstanceOf[List[T]]

  override def isLazy = false

  override def withOption(opt: CompileOption): RowBasedData[ROW] = new ListData(list, opt)

  override def distinct() = list.distinct

  override def join[T: ClassTag](other: RowBasedData[T], jointPoint: (Row) => Boolean, rowifier: (ROW, T) => Row): RowBasedData[Row] = {
    if (this.isInstanceOf[RowBasedData[Row]]) {
      other match {
        case rbd: ListData[Row] =>
          val newList = list.asInstanceOf[List[Row]].flatMap {
            t1 =>
              rbd.list.flatMap {
                t2 =>
                  val allData = t1.data ++ t2.data
                  val newRow = new Row(allData)
                  if (jointPoint.apply(newRow)) {
                    Seq(newRow)
                  } else {
                    None
                  }
              }
          }
          newList
        case _ =>
          throw new IllegalArgumentException("Joining with " + other.getClass + " is not supported")
      }
    } else {
      throw new IllegalArgumentException("Can only join if this is instance of RowBasedData[Row]")
    }
  }

}