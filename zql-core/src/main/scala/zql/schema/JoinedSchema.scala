package zql.schema

import org.slf4j.LoggerFactory
import zql.core.{ ColumnRef, Table }

class JoinedSchema(tb1: Table, tb2: Table) extends Schema(tb1.name + "_" + tb2.name) {
  val logger = LoggerFactory.getLogger(classOf[JoinedSchema])

  override def allColumns(): Seq[ColumnDef] = {
    val tb1Cols = tb1.schema.allColumns()
    val tb2Cols = tb2.schema.allColumns()
    tb1Cols ++ tb2Cols
  }
  logger.debug("all columns = " + allColumns().map(_.name).mkString(", "))

  override def resolveColumnDef(colName: Symbol): ColumnRef = {
    val colDef1 = tb1.schema.resolveColumnDef(colName)
    val colDef2 = tb2.schema.resolveColumnDef(colName)
    (colDef1, colDef2) match {
      case (a: ColumnRef, null) => a
      case (null, b: ColumnRef) => b
      case (a: ColumnRef, b: ColumnRef) => throw new IllegalArgumentException("Ambiguous column " + colName.name)
      case (null, null) => throw new IllegalArgumentException("Unknown column " + colName.name)
    }
  }
}
