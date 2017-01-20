package zql.rowbased

import java.util.UUID

import zql.core._
import zql.schema.Schema

import scala.reflect.ClassTag

abstract class RowBasedTable[R: ClassTag](val schema: Schema) extends Table {
  def id = UUID.randomUUID().toString

  override def toString = name + id

  def data: RowBasedData[R]

  override def compile(stmt: Statement): Executable[Table] = getCompiler.compile(stmt, schema)

  override def as(alias: Symbol): Table = createTable[R](schema.as(alias), data)

  def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T]

  def collectAsList[T: ClassTag]() = data.asList[T]

  def getCompiler[TB <: Table]() = new RowBasedStatementCompiler[R](this).asInstanceOf[StatementCompiler[TB]]

  protected def joinWithType2[T](table: Table, jt: JoinType)(implicit tag: reflect.ClassTag[T]): JoinedTable = table match {
    case rbt: RowBasedTable[T] =>
      new JoinedRowBasedTable(this, rbt, jt)
    case _ =>
      throw new IllegalArgumentException("Cannot join rowbased table with " + table + " of type " + table.getClass)
  }

  protected def joinWithType(table: Table, jt: JoinType): JoinedTable = joinWithType2(table, jt)

  override def collectAsRowList = collectAsList[Row]
}
