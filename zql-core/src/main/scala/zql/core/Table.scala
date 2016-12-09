package zql.core

import java.lang.reflect.Field
import java.util.UUID

import scala.reflect.ClassTag

/**
 * Created by tiong on 6/2/16.
 */

abstract class Table {
  def schema: Schema

  def name: String

  def compile(stmt: Statement): Executable[Table]

  def collectAsList(): List[Any]

  def join[TB <: Table](table: TB) = new JoinedTable[this.type, TB](this, table)
}

class JoinedTable[T1 <: Table, T2 <: Table](val t1: T1, val t2: T2) extends Table {

  override def name: String = "Join[" + t1.name + "," + t2.name + "]"

  override def schema: Schema = {
    val cols = t1.schema.allColumns() ++ t2.schema.allColumns()
    new DefaultSchema(cols: _*)
  }

  override def collectAsList(): List[Any] = ???

  override def compile(stmt: Statement): Executable[Table] = ???
}

abstract class TypedTable[T](val schema: DefaultSchema) extends Table

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

  override lazy val hashCode = data.map(_.hashCode()).sum

  override def equals(obj: scala.Any): Boolean = if (obj == null) false else obj.asInstanceOf[Row].data.sameElements(data)

  override def toString = data.mkString(",")
}

