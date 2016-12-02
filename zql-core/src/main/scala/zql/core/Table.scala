package zql.core

import java.lang.reflect.Field

import scala.reflect.ClassTag

/**
 * Created by tiong on 6/2/16.
 */

abstract class Table {
  def schema: Schema

  def select(selects: Column*): Selected = new Selected(selects, this)

  def compile(stmt: Statement): Executable[Table]

  def collectAsList(): List[Any]
}

abstract class TypedTable[T](val schema: RowBasedSchema[T]) extends Table

class EmptyRow(array: Array[Any]) extends Row(array) {
  override def aggregate(row: Row, indices: Array[Int]): Row = row
}

case class Row(val data: Array[Any]) {
  def aggregate(row: Row, indices: Array[Int]): Row = {
    //TODO: make sure this won't have side effect as we use shallow copy
    val newRow = new Row(data)
    indices.foreach { i =>
      val a = data(i).asInstanceOf[Aggregatable[Any]]
      val b = row.data(i).asInstanceOf[Aggregatable[Any]]
      newRow.data(i) = a.aggregate(b)
    }
    newRow
  }

  def normalize: Row = {
    data.zipWithIndex.map {
      case (value, index) =>
        value match {
          case s: Aggregatable[_] =>
            data(index) = s.value
          case any: AnyRef =>
            any
        }
    }
    this
  }

  override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[Row].data.sameElements(data)

  override def toString = data.mkString(",")
}

