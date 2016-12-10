package zql.core

abstract class Table {
  def schema: Schema

  def name: String

  def alias: String

  def as(alias: Symbol): Table

  def compile(stmt: Statement): Executable[Table]

  def collectAsList(): List[Any]

  def join(table: Table): JoinedTable = ???
}

class AliasSchema(schema: Schema, alias: Symbol) extends Schema {
  val allColumns = schema.allColumns()

  val prefix = alias.name + "_"

  override def resolveColumnDef(symbol: Symbol) = {
    if (columnMap.contains(symbol)) {
      columnMap(symbol)
    } else if (symbol.name.startsWith(prefix)) {
      val colName = Symbol(symbol.name.substring(prefix.length))
      if (columnMap.contains(colName)) {
        columnMap(colName)
      } else throw new IllegalArgumentException("Unknown column " + symbol.name)
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

class JoinedTable(val t1: Table, val t2: Table) extends Table {

  override def name: String = "Join[" + t1.name + "," + t2.name + "]"

  //  class JoinedSchema(t1, t2) {
  //
  //  }
  override def schema: Schema = {
    val cols = t1.schema.allColumns() ++ t2.schema.allColumns()
    new DefaultSchema(cols: _*)
  }

  override def collectAsList(): List[Any] = ???

  override def compile(stmt: Statement): Executable[Table] = ???

  override def join(table: Table): JoinedTable = ???

//  override def getCompiler[T <: Table](): Compiler[T] = ???
  override def as(alias: Symbol): Table = ???

  override def alias: String = ???
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

