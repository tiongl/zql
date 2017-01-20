package zql.core

import zql.rowbased.RowColumnDef
import zql.schema.{ DefaultSchema, Schema }

import scala.collection.mutable

case class StatementInfo(stmt: Statement, schema: Schema) {
  val expandedSelects = stmt.select.flatMap {
    case mc: MultiColumn =>
      mc.toColumns(schema)
    case c => Seq(c)
  }

  val selectColumnNames = mutable.LinkedHashSet(expandedSelects.flatMap(_.requiredColumns): _*).toList //unique selects

  val allRequiredColumnNames: List[Symbol] = if (stmt.from == null) List() else {
    val columns = mutable.LinkedHashSet(selectColumnNames: _*) //for dedupling
    if (stmt.from != null) {
      stmt.from match {
        case jt: JoinedTable =>
          if (jt.jointPoint != null) {
            columns ++= jt.jointPoint.requiredColumns
          }
        case _ =>
        //do nothing
      }
    }
    if (stmt.where != null) {
      columns ++= stmt.where.requiredColumns
    }
    if (stmt.groupBy != null) {
      columns ++= stmt.groupBy.flatMap(_.requiredColumns)
    }
    if (stmt.having != null) {
      columns ++= stmt.having.requiredColumns
    }
    if (stmt.orderBy != null) {
      columns ++= stmt.orderBy.flatMap(_.requiredColumns)
    }
    columns.toList
  }

  val joinColumnDefs: Seq[(Symbol, ColumnRef)] = stmt.from match {
    case jt: JoinedTable =>
      if (jt.jointPoint != null) {
        jt.jointPoint.requiredColumns.map {
          sym => (sym, stmt.from.schema.resolveColumnDef(sym))
        }
      } else Seq()
    case _ =>
      Seq()
  }

  val columnDefs: Seq[(Symbol, ColumnRef)] =
    allRequiredColumnNames.map {
      sym =>
        (sym, stmt.from.schema.resolveColumnDef(sym))
    }

  val columnDefsMap = columnDefs.toMap

  val groupByIndices = expandedSelects.zipWithIndex.filter(_._1.isInstanceOf[AggregateFunction[_]]).map(_._2).toArray

  val newColumns = expandedSelects.zipWithIndex.map {
    case (col, index) =>
      if (columnDefsMap.contains(col.name)) {
        val colRef = columnDefsMap(col.name)
        new RowColumnDef(col.getResultName, index, colRef.colDef.dataType)
      } else {
        new RowColumnDef(col.getResultName, index, classOf[Any])
      }
  }

  val resultSchema = new DefaultSchema(schema.name + ".result", newColumns)

}
