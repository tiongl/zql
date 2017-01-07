package zql.list

import java.util.UUID

import zql.core._
import zql.core.util.Utils
import scala.reflect.ClassTag

class ListTable[ROW: ClassTag](schema: Schema, list: List[ROW]) extends RowBasedTable[ROW](schema) {

  val data = new ListData(list)

  override def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): ListTable[T] = {
    val list = rowBased.asInstanceOf[ListData[T]].list
    new ListTable(newSchema, list)
  }

  override def as(alias: Symbol): Table = new ListTable[ROW](schema.as(alias), list)
}

class TypedFunc[ROW, T: ClassTag](func: (ROW) => T)

class ListData[ROW](val list: List[ROW], val option: CompileOption = new CompileOption) extends RowBasedData[ROW] {

  implicit def listToListData[T](list: List[T]) = new ListData[T](list, option)

  override def select(r: (ROW) => Row): RowBasedData[Row] = list.map(r).toList

  override def filter(filter: (ROW) => Boolean): RowBasedData[ROW] = list.filter(filter)

  override def groupBy(keyFunc: (ROW) => Row, valueFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row] = {
    Utils.groupBy[ROW, Row](list, keyFunc(_), valueFunc(_), _.aggregate(_, aggregatableIndices)).map(_.normalize).toList
  }

  override def reduce(reduceFunc: (ROW, ROW) => ROW) = List(list.reduce(reduceFunc))

  override def map(mapFunc: (ROW) => Row) = list.map(mapFunc)

  override def sortBy(keyFunc: (ROW) => Row, ordering: Ordering[Row], ctag: ClassTag[Row]) = new ListData(list.sortBy(keyFunc)(ordering))

  override def slice(offset: Int, until: Int) = list.slice(offset, until)

  override def size() = list.length

  override def asList = list

  override def isLazy = false

  override def withOption(opt: CompileOption): RowBasedData[ROW] = new ListData(list, opt)

  override def distinct() = list.distinct

  override def join(other: RowBasedData[Row], jointPoint: (Row) => Boolean): RowBasedData[Row] = {
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