package zql.core

import zql.schema.Schema
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

