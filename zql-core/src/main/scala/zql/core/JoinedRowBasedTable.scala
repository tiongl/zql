package zql.core

import org.slf4j.{ LoggerFactory, Logger }
import zql.sql.SqlGenerator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class JoinedRowBasedTable[T1, T2](tb1: RowBasedTable[T1], tb2: RowBasedTable[T2]) extends JoinedTable(tb1, tb2) with AccessorCompiler {
  override def schema: Schema = new JoinedSchema(tb1, tb2)

  override def alias: String = ???

  override def name: String = ???

  override def collectAsList(): List[Any] = ???

  override def compile(stmt: Statement): Executable[Table] = {
    //first get all the columns we need
    val info = new StatementInfo(stmt, schema)
    val com1 = new RowBasedCompiler(tb1)
    val com2 = new RowBasedCompiler(tb2)
    val select1 = ArrayBuffer[ColumnAccessor[T1, Any]]()
    val select2 = ArrayBuffer[ColumnAccessor[T2, Any]]()
    val allColumns = info.allRequiredColumnNames.map {
      colName =>
        val colDef1 = Try(tb1.schema.resolveColumnDef(colName)).getOrElse(null)
        val colDef2 = Try(tb2.schema.resolveColumnDef(colName)).getOrElse(null)
        if (colDef1 != null) {
          if (colDef2 != null) {
            throw new IllegalArgumentException("Ambiguous column " + colName.name)
          } else {
            val accessor = new ColumnAccessor[T1, Any] {
              override def apply(v1: T1): Any = colDef1.asInstanceOf[TypedColumnDef[Any]](v1)
            }
            select1 += accessor
          }
        } else if (colDef2 == null) {
          throw new IllegalArgumentException("Unknown column " + colName.name)
        } else {
          val accessor = new ColumnAccessor[T2, Any] {
            override def apply(v1: T2): Any = colDef2.asInstanceOf[TypedColumnDef[Any]](v1)
          }
          select2 += accessor
        }
    }
    val selectFunc1 = (t1: T1) => new Row(select1.map(_(t1)).toArray)
    val selectFunc2 = (t1: T2) => new Row(select2.map(_(t1)).toArray)

    val d1 = tb1.data.select(selectFunc1)
    val d2 = tb2.data.select(selectFunc2)

    val newCols = info.allRequiredColumnNames.zipWithIndex.map {
      case (name, i) => new RowColumnDef(name, i)
    }
    val resultSchema = new DefaultSchema(newCols: _*)
    val joinExtractor = if (jointPoint != null) {
      compileCondition[Row](jointPoint, resultSchema)
    } else {
      new ColumnAccessor[Row, Boolean] {
        override def apply(v1: Row): Boolean = true
      }
    }

    val crossProduct = d1.join(d2, (r: Row) => joinExtractor.apply(r))
    val newTable = tb1.createTable(crossProduct, new DefaultSchema(newCols: _*))
    newTable.compile(stmt)
  }

  override def as(alias: Symbol): Table = ???
}

class JoinedRowBasedCompiler(table: JoinedRowBasedTable[_, _])

