package zql.core

import java.lang.reflect.Field

import scala.reflect.ClassTag

/**
  * Created by tiong on 6/2/16.
  */


abstract class Table(val schema: Schema[_]) {
  def select(selects: Column*): Selected = new Selected(selects, this)

  def compile(stmt: Statement): Executable[Table]

  def compileSelect[IN](col: Column): Seq[ColumnAccessor[IN]]

  def compileColumn[IN](col: Column): ColumnAccessor[IN]

  def collectAsList(): List[Any]
}





case class Row(val data: Array[Any]){
  def aggregate(row: Row, indices: Array[Int]): Row = {
    //TODO: make sure this won't have side effect as we use shallow copy
    val newRow = new Row(data)
    indices.foreach{i =>
      val a = data(i).asInstanceOf[Aggregatable]
      val b = row.data(i).asInstanceOf[Aggregatable]
      newRow.data(i) = a.aggregate(b)
    }
    newRow
  }


  override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[Row].data.sameElements(data)

  override def toString = data.mkString(",")
}




