package zql.core

import org.slf4j.{ LoggerFactory, Logger }
import zql.sql.SqlGenerator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class JoinedRowBasedTable[T1, T2](tb1: RowBasedTable[T1], tb2: RowBasedTable[T2], val alias: String = null) extends JoinedTable(tb1, tb2) with AccessorCompiler {

  val table = this

  override def schema: Schema = new JoinedSchema(tb1, tb2)

  override def name: String = s"joined_${tb1.name}_${tb2.name}"

  override def collectAsList(): List[Any] = ???

  override def compile(stmt: Statement): Executable[Table] = {
    //first get all the columns we need
    val info = new StatementInfo(stmt, schema)
    val com1 = new RowBasedCompiler(tb1)
    val com2 = new RowBasedCompiler(tb2)
    val t1Cols = mutable.LinkedHashMap[Symbol, ColumnDef]()
    val t2Cols = mutable.LinkedHashMap[Symbol, ColumnDef]()

    val allColumnDefs = info.allRequiredColumnNames.map {
      colName =>
        val flags =
          (
            Try(tb1.schema.resolveColumnDef(colName, tb1.getPrefixes())).getOrElse(null),
            Try(tb2.schema.resolveColumnDef(colName, tb2.getPrefixes())).getOrElse(null)
          )
        flags match {
          case (a: ColumnDef, null) =>
            t1Cols.put(colName, a)
          case (null, b: ColumnDef) =>
            t2Cols.put(colName, b)
          case (a: ColumnDef, b: ColumnDef) =>
            throw new IllegalArgumentException("Ambiguous column " + colName.name)
          case (null, null) =>
            throw new IllegalArgumentException("Unknown column " + colName.name)
        }
    }

    val select1 = t1Cols.values.map { colDef =>
      new ColumnAccessor[T1, Any] {
        override def apply(v1: T1): Any = colDef.asInstanceOf[TypedColumnDef[Any]].get(v1)
      }
    }

    val select2 = t2Cols.values.map { colDef =>
      new ColumnAccessor[T2, Any] {
        override def apply(v1: T2): Any = colDef.asInstanceOf[TypedColumnDef[Any]].get(v1)
      }
    }
    val selectFunc1 = (t1: T1) => new Row(select1.map(_(t1)).toArray)
    val selectFunc2 = (t1: T2) => new Row(select2.map(_(t1)).toArray)

    val d1 = tb1.data.select(selectFunc1)
    val d2 = tb2.data.select(selectFunc2)

    val allColumns = t1Cols.keysIterator ++ t2Cols.keysIterator
    val newCols = allColumns.zipWithIndex.map {
      case (name, i) => new RowColumnDef(name, i)
    }.toSeq
    val resultSchema = new DefaultSchema(newCols: _*)
    val joinExtractor = if (jointPoint != null) {
      compileCondition[Row](jointPoint, resultSchema)
    } else {
      new ColumnAccessor[Row, Boolean] {
        override def apply(v1: Row): Boolean = true
      }
    }

    val crossProduct = d1.join(d2, (r: Row) => joinExtractor.apply(r))
    val newTable = tb1.createTable(name + ".temp", crossProduct, new DefaultSchema(newCols: _*))
    newTable.compile(stmt)
  }

  override def as(alias: Symbol): Table = new JoinedRowBasedTable[T1, T2](tb1, tb2, alias.name)
}

class JoinedRowBasedCompiler(table: JoinedRowBasedTable[_, _])

