package zql.rowbased

import java.util.UUID

import zql.core._
import zql.schema.Schema

import scala.reflect.ClassTag

abstract class RowBasedTable[R: ClassTag](val schema: Schema) extends Table {
  def id = UUID.randomUUID().toString

  override def toString = name + id

  def data: RowBasedData[R]

  override def compile(stmt: Statement): Executable[Table] = {
    getCompiler.compile(stmt, schema)
  }

  override def as(alias: Symbol): Table = createTable[R](schema.as(alias), data)

  def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T]

  def collectAsList() = data.asList

  def getCompiler[TB <: Table]() = new RowBasedCompiler[R](this).asInstanceOf[Compiler[TB]]

  override def join(table: Table): JoinedTable = table match {
    case rbt: RowBasedTable[_] =>
      new JoinedRowBasedTable(this, rbt)
    case _ =>
      throw new IllegalArgumentException("Cannot join rowbased table with " + table + " of type " + table.getClass)
  }
}
