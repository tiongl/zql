package zql.sql

import zql.core.Statement
import zql.util.ColumnVisitor

abstract class SqlGenerator extends ColumnVisitor[String, String] {
  def generateSql(stmt: Statement): String
}
