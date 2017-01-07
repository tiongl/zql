package zql.core

import zql.core.util.Utils
import zql.sql.SqlGenerator

abstract class Table {

  def schema: Schema

  def as(alias: Symbol): Table

  def join(table: Table): JoinedTable

  def name: String = schema.name

  def alias: String = schema.alias

  def compile(stmt: Statement): Executable[Table]

  def collectAsList(): List[Any]

  def toSql(gen: SqlGenerator): String = {
    val builder = new StringBuilder
    builder.append(name)
    if (alias != null) {
      builder.append(" AS " + alias)
    }
    builder.toString
  }

}

case class ColumnRef(val schema: Schema, val colDef: ColumnDef)

abstract class JoinedTable(val tb1: Table, val tb2: Table) extends Table {
  var jointPoint: Condition = null

  def on(cond: Condition): JoinedTable = {
    if (jointPoint == null) {
      jointPoint = cond
    } else {
      throw new IllegalStateException("Join-point has been set")
    }
    this
  }

  override def toSql(gen: SqlGenerator): String = {
    val sb = new StringBuilder
    val t1 = tb1.toSql(gen)
    val t2 = tb2.toSql(gen)
    sb.append(t1 + " JOIN " + t2)
    if (jointPoint != null) {
      sb.append(" ON ")
      sb.append(gen.visit(jointPoint, null))
    }
    return sb.toString
  }

}

class EmptyRow(array: Array[Any]) extends Row(array) {
  override def aggregate(row: Row, indices: Array[Int]): Row = row
}

object Row {
  implicit val rowOrdering = new RowOrdering()
}

case class Row(val data: Array[Any]) extends Comparable[Row]{

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

  override def compareTo(o: Row): Int = Row.rowOrdering.compare(this, o)
}


class RowOrdering(ascendings: Array[Boolean] = Array()) extends Ordering[Row] {
  override def compare(x: Row, y: Row): Int = {
    x.data.zip(y.data).zipWithIndex.foreach {
      case ((a, b), index) =>
        val comparedValue = Utils.compare(a, b)
        if (comparedValue != 0) {
          if (ascendings.length<=index || ascendings(index)) {
            return comparedValue
          } else {
            return comparedValue * -1
          }

        }
    }
    0
  }
}

