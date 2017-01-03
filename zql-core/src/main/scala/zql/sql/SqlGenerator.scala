package zql.sql

import org.slf4j.LoggerFactory
import zql.core.{ ColumnVisitor, Statement, ColumnTraverser }

abstract class SqlGenerator extends ColumnVisitor[String, String] {
  def generateSql(stmt: Statement): String
}
