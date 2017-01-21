package zql.rowbased

import zql.core._
import zql.schema.{ DefaultSchema, JoinedSchema, Schema }
import zql.sql.DefaultSqlGenerator
import zql.util.{ ColumnTraverser, ColumnVisitor }

import scala.collection.mutable
import scala.reflect.ClassTag

class JoinedRowBasedTable[T1: ClassTag, T2: ClassTag](tb1: RowBasedTable[T1], tb2: RowBasedTable[T2], jt: JoinType) extends JoinedTable(tb1, tb2, jt) with AccessorCompiler {

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
    val selectFunc1 = RowBasedStatementCompiler.toRowBuilder[T1](select1)
    val selectFunc2 = RowBasedStatementCompiler.toRowBuilder[T2](select2)

    //TODO: Optimize with table specific filter
    //    val d1 = tb1.data.withOption(option).map(selectFunc1)
    //    val d2 = tb2.data.withOption(option).map(selectFunc2)

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

    //    val crossProduct = d1.withOption(option).join(d2, (r: Row) => joinExtractor.apply(r))

    val joinedData: RowBasedData[Row] = if (jointPoint == null) {
      tb1.data.crossJoin(tb2.data, selectFunc1, selectFunc2)
    } else {
      val (leftKey, rightKey) = analyzeJointpoint[T1, T2](jointPoint)
      tb1.data.withOption(option).joinWithKey(tb2.data, leftKey, rightKey, selectFunc1, selectFunc2, joinType)
    }
    val newTable = tb1.createTable(resultSchema, joinedData)
    newTable.compile(stmt)
  }

  def analyzeJointpoint[T, U](joinPoint: Condition): (RowBuilder[T], RowBuilder[U]) = {
    val analyzer = new JoinAnalyzer(this, tb1, tb2)
    analyzer.traverse(joinPoint, new Object)
    val (leftCols, rightCols) = (analyzer.tb1Columns, analyzer.tb2Columns)
    val leftFunc = RowBasedStatementCompiler.toRowBuilder(leftCols.map(compileColumn[T](_, tb1.schema)))
    val rightFunc = RowBasedStatementCompiler.toRowBuilder(rightCols.map(compileColumn[U](_, tb2.schema)))
    (leftFunc, rightFunc)
  }

  override def join(table: Table): JoinedTable = ???
  //  table match {
  //    case rbt: RowBasedTable[_] =>
  //      new JoinedRowBasedTable(this, rbt)
  //    case _ =>
  //      throw new IllegalArgumentException("Cannot join rowbased table with " + table + " of type " + table.getClass)
  //  }
}

class JoinedRowBasedCompiler(table: JoinedRowBasedTable[_, _])

class JoinAnalyzer(jtb: JoinedTable, tb1: Table, tb2: Table) extends ColumnTraverser[AnyRef, AnyRef]("Only equals and and are allowed in join") {
  val tb1Columns = mutable.ArrayBuffer[Column]()
  val tb2Columns = mutable.ArrayBuffer[Column]()

  override def handleAndCondition(ac: AndCondition, context: AnyRef): AnyRef = {
    ac.cols.map(traverse(_, context))
  }

  override def handleEquals(ec: EqualityCondition, context: AnyRef): AnyRef = {
    val aRef = resolveReference(ec.a)
    val bRef = resolveReference(ec.b)
    val flags = (aRef.schema == tb1.schema, bRef.schema == tb2.schema, aRef.schema == tb2.schema, bRef.schema == tb1.schema)
    println(flags)
    flags match {
      case (true, true, _, _) =>
        tb1Columns += ec.a
        tb2Columns += ec.b
      case (_, _, true, true) =>
        tb1Columns += ec.b
        tb2Columns += ec.a
      case _ =>
        throw new UnsupportedOperationException("Invalid join " + ec.toSql(new DefaultSqlGenerator))
    }
  }

  def resolveReference(col: Column) = col match {
    case dc: DataColumn =>
      jtb.schema.resolveColumnDef(dc.name)
    case _ =>
      throw new IllegalArgumentException("Unsupported join-point " + col + " type " + col.getClass.getSimpleName)

  }
}
