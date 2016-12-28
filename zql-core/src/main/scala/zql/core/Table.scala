package zql.core

import zql.sql.SqlGenerator

abstract class Table {
  def schema: Schema

  def name: String

  def alias: String

  def as(alias: Symbol): Table

  def compile(stmt: Statement): Executable[Table]

  def collectAsList(): List[Any]

  def join(table: Table): JoinedTable = ???

  def toSql(gen: SqlGenerator): String = {
    val builder = new StringBuilder
    builder.append(name)
    if (alias != null) {
      builder.append(" AS " + alias)
    }
    builder.toString
  }
}

class AliasSchema(schema: Schema, alias: Symbol) extends Schema {

  val properPrefix = alias.name + "."

  val prefix = alias.name + "_"

  val allColumns = schema.allColumns().map(c => c.rename(Symbol(prefix + c.name.name)))

  override def resolveColumnDef(symbol: Symbol) = {
    if (columnMap.contains(symbol)) {
      columnMap(symbol)
    } else if (symbol.name.startsWith(prefix)) {
      val colName = Symbol(symbol.name.substring(prefix.length))
      val dotName = Symbol(properPrefix + colName.name)
      if (columnMap.contains(colName)) {
        columnMap(colName)
      } else if (columnMap.contains(dotName)) {
        columnMap(dotName)
      } else {
        throw new IllegalArgumentException("Unknown column " + symbol.name)
      }
    } else throw new IllegalArgumentException("Unknown column " + symbol.name)
  }
}

object AliasSchema {
  def apply(schema: Schema, alias: Symbol) = schema match {
    case as: AliasSchema =>
      throw new IllegalArgumentException("Realiasing is not supported")
    case _ =>
      new AliasSchema(schema, alias)
  }
}

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

