package zql.core

import zql.rowbased.Row
import zql.schema.Schema
import zql.sql.SqlGenerator

import scala.reflect.ClassTag

abstract class Table {

  def schema: Schema

  def as(alias: Symbol): Table

  def join(table: Table): JoinedTable = innerJoin(table)

  protected def joinWithType(table: Table, jt: JoinType): JoinedTable

  def innerJoin(table: Table): JoinedTable = joinWithType(table, JoinType.innerJoin)

  def leftJoin(table: Table): JoinedTable = joinWithType(table, JoinType.leftJoin)

  def rightJoin(table: Table): JoinedTable = joinWithType(table, JoinType.rightJoin)

  def fullJoin(table: Table): JoinedTable = joinWithType(table, JoinType.fullJoin)

  def name: String = schema.name

  def alias: String = schema.alias

  def compile(stmt: Statement): Executable[Table]

  def collectAsList[T: ClassTag](): List[T]

  def collectAsRowList: List[Row]

  def toSql(gen: SqlGenerator): String = {
    val builder = new StringBuilder
    builder.append(name)
    if (alias != null) {
      builder.append(" AS " + alias)
    }
    builder.toString
  }

}

