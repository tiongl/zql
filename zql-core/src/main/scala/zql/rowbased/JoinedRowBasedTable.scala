package zql.rowbased

import zql.core._
import zql.schema.{ DefaultSchema, JoinedSchema, Schema }

import scala.reflect.ClassTag

class JoinedRowBasedTable[T1, T2](tb1: RowBasedTable[T1], tb2: RowBasedTable[T2]) extends JoinedTable(tb1, tb2) with AccessorCompiler {

  val table = this

  override def schema: Schema = new JoinedSchema(tb1, tb2)

  override def name: String = s"joined_${tb1.name}_${tb2.name}"

  override def collectAsList[T: ClassTag](): List[T] = ???

  override def collectAsRowList = ???

  override def compile(stmt: Statement): Executable[Table] = {
    //first get all the columns we need
    val option = new CompileOption
    option.put("stmtInfo", new StatementInfo(stmt, schema))
    val info = new StatementInfo(stmt, schema)
    val com1 = new RowBasedStatementCompiler(tb1)
    val com2 = new RowBasedStatementCompiler(tb2)

    val allColumnDefs: Seq[(Symbol, ColumnRef)] = info.columnDefs
    val t1Cols = allColumnDefs.filter(_._2.schema == tb1.schema)
    val t2Cols = allColumnDefs.filter(_._2.schema == tb2.schema)

    val select1 = t1Cols.map(_._2.colDef).map { colDef =>
      new ColumnAccessor[T1, Any] {
        override def apply(v1: T1): Any = colDef.asInstanceOf[RowBasedColumnDef[Any]].get(v1)
      }
    }

    val select2 = t2Cols.map(_._2.colDef).map { colDef =>
      new ColumnAccessor[T2, Any] {
        override def apply(v1: T2): Any = colDef.asInstanceOf[RowBasedColumnDef[Any]].get(v1)
      }
    }
    val selectFunc1 = (t1: T1) => new Row(select1.map(_(t1)).toArray)
    val selectFunc2 = (t2: T2) => new Row(select2.map(_(t2)).toArray)

    val d1 = tb1.data.withOption(option).map(selectFunc1)
    val d2 = tb2.data.withOption(option).map(selectFunc2)

    val allColumns = t1Cols ++ t2Cols
    val newCols = allColumns.zipWithIndex.map {
      case ((name, colRef), i) => new RowColumnDef(name, i, colRef.colDef.dataType)
    }.toSeq
    val resultSchema = new DefaultSchema(name + ".result", newCols)

    option.put("resultSchema", resultSchema)

    val joinExtractor = if (jointPoint != null) {
      compileCondition[Row](jointPoint, resultSchema)
    } else {
      new ColumnAccessor[Row, Boolean] {
        override def apply(v1: Row): Boolean = true
      }
    }

    val crossProduct = d1.withOption(option).join(d2, (r: Row) => joinExtractor.apply(r))
    val newTable = tb1.createTable(resultSchema, crossProduct)
    newTable.compile(stmt)
  }

  override def as(alias: Symbol): Table = throw new IllegalStateException("Alias of joined table is not supported")

  override def join(table: Table): JoinedTable = ???
  //  table match {
  //    case rbt: RowBasedTable[_] =>
  //      new JoinedRowBasedTable(this, rbt)
  //    case _ =>
  //      throw new IllegalArgumentException("Cannot join rowbased table with " + table + " of type " + table.getClass)
  //  }
}

class JoinedRowBasedCompiler(table: JoinedRowBasedTable[_, _])
