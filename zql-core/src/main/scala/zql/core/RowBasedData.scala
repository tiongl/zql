package zql.core

import java.util.UUID

import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

trait RowBasedData[ROW] {
  def option: CompileOption
  def withOption(option: CompileOption): RowBasedData[ROW]
  def slice(offset: Int, until: Int): RowBasedData[ROW]
  def select(r: (ROW) => Row): RowBasedData[Row]
  def filter(filter: (ROW) => Boolean): RowBasedData[ROW]
  def groupBy(keyFunc: (ROW) => Row, selectFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row]
  def reduce(reduceFunc: (ROW, ROW) => ROW): RowBasedData[ROW]
  def map(mapFunc: (ROW) => Row): RowBasedData[Row]
  def sortBy(keyFunc: (ROW) => Row, ordering: Ordering[Row], tag: ClassTag[Row]): RowBasedData[ROW]
  def size: Int
  def asList: List[ROW]
  def isLazy: Boolean
  def distinct(): RowBasedData[ROW]
  def join(other: RowBasedData[Row], jointPoint: (Row) => Boolean): RowBasedData[Row]
}

class DefaultSchema(name: String, val allColumns: Seq[ColumnDef], alias: String = null) extends Schema(name, alias) {
  override def toString = "DefaultSchema[" + allColumns.map(_.name.name).mkString(",") + "]"
}

abstract class Getter {
  def get(obj: Any): Any
  def dataType: Class[_]
}

//NOTE: lazy field are mainly to accomodate distributed computation because Method/Field are not serializable
case class LazyField(className: String, fieldName: String) extends Getter {
  @transient lazy val field = Class.forName(className).getField(fieldName)
  lazy val dataType = field.getType
  def get(obj: Any) = field.get(obj)
}

case class LazyMethod(className: String, methodName: String) extends Getter {
  @transient lazy val method = {
    val clazz = Class.forName(className)
    if (clazz == null) {
      throw new IllegalStateException("Cannot find class " + className)
    } else {
      clazz.getMethod(methodName)
    }
  }

  lazy val dataType = method.getReturnType

  def get(obj: Any) = method.invoke(obj)
}

abstract class TypedColumnDef[R](name: Symbol) extends ColumnDef(name) {
  def get(v1: R): Any
}


class FuncColumnDef[R](name: Symbol, func: (R) => Any) extends TypedColumnDef[R](name) {
  override def get(v1: R): Any = func.apply(v1)

  val dataType = classOf[Any]

  override def rename(newName: Symbol): ColumnDef = new FuncColumnDef[R](newName, func)

  def ass(sym: Symbol) = rename(sym)
}

class RowColumnDef(name: Symbol, i: Int, val dataType: Class[_]) extends TypedColumnDef[Row](name) {
  override def get(v1: Row): Any = v1.data(i)
  override def rename(newName: Symbol) = new RowColumnDef(newName, i, dataType)
  override def toString = s"RowColumnDef(${name},${i})"
}

class ReflectedColumnDef[T: ClassTag](name: Symbol, getter: Getter) extends TypedColumnDef[T](name) {

  override def get(v1: T): Any = getter.get(v1)

  override def dataType = getter.dataType

  override def rename(newName: Symbol): ColumnDef = new ReflectedColumnDef[T](newName, getter)
}

abstract class RowBasedTable[R: ClassTag](val schema: Schema) extends Table {
  def id = UUID.randomUUID().toString

  override def toString = name + id

  def data: RowBasedData[R]

  override def compile(stmt: Statement): Executable[Table] = {
    getCompiler.compile(stmt, schema)
  }

  override def as(alias: Symbol): Table = createTable[R](schema.as(alias), data)

  def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T]

  def collectAsList() = data.asList

  def getCompiler[TB <: Table]() = new RowBasedCompiler[R](this).asInstanceOf[Compiler[TB]]

  override def join(table: Table): JoinedTable = table match {
    case rbt: RowBasedTable[_] =>
      new JoinedRowBasedTable(this, rbt)
    case _ =>
      throw new IllegalArgumentException("Cannot join rowbased table with " + table + " of type " + table.getClass)
  }
}

/** allow accessing a column from type T **/
abstract class ColumnAccessor[ROW, +T]() extends ((ROW) => T) with Serializable

abstract class ConditionAccessor[ROW]() extends ColumnAccessor[ROW, Boolean]

case class StatementInfo(stmt: Statement, schema: Schema) {
  val expandedSelects = stmt.select.flatMap {
    case mc: MultiColumn =>
      mc.toColumns(schema)
    case c => Seq(c)
  }

  val selectColumnNames = mutable.LinkedHashSet(expandedSelects.flatMap(_.requiredColumns): _*).toList //unique selects

  val allRequiredColumnNames: List[Symbol] = if (stmt.from==null) List() else
  {
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


object RowBasedCompiler {
  def toRowFunc[ROW](selects: Seq[ColumnAccessor[ROW, _]]) = (row: ROW) => new Row(selects.map(s => s.apply(row)).toArray)
  def toFunc[ROW, T](accessor: ColumnAccessor[ROW, T]): (ROW) => T = (row: ROW) => accessor.apply(row)
}

class RowBasedCompiler[ROW](val table: RowBasedTable[ROW]) extends Compiler[RowBasedTable[Row]] with AccessorCompiler {

  def validate(info: StatementInfo) = {
    val stmt = info.stmt
    if (stmt.groupBy != null) {
      val aggFunctions = stmt.select.filter(_.isInstanceOf[AggregateFunction[_]])
      if (aggFunctions.size == 0) {
        throw new IllegalArgumentException("Group by must have at least one aggregation function")
      }

      //make sure groupby is part of selects
      //TODO: determine whether this is really needed
      val groupByNames = stmt.groupBy.map(_.name).toSet
      val selectNames = info.expandedSelects.map(_.name).toSet
      if (!groupByNames.subsetOf(selectNames)) {
        throw new IllegalArgumentException("Group by must be present in selects: " + groupByNames.diff(selectNames))
      }
      //make sure all are aggregation function
      info.expandedSelects.map {
        col =>
          if (!groupByNames.contains(col.name) && !col.isInstanceOf[AggregateFunction[_]]) {
            throw new IllegalArgumentException("Select must be aggregate function if it is not group by")
          }
      }
    } else if (info.groupByIndices.size > 0) {
      if (info.expandedSelects.size > info.groupByIndices.size) {
        throw new IllegalArgumentException("All select must be aggregate function (or use first() udf)")
      }
    }
  }

  def compile(stmt: Statement, schema: Schema, option: CompileOption): Executable[RowBasedTable[Row]] = {
    import zql.core.ExecutionPlan._
    val stmtInfo = new StatementInfo(stmt, schema)
    validate(stmtInfo)
    option.put("stmtInfo", stmtInfo)
    val selects = stmtInfo.expandedSelects.map(c => compileColumn[ROW](c, schema, "SELECT"))
    val filterAccessor: ColumnAccessor[ROW, Boolean] = if (stmt.where != null) compileCondition[ROW](stmt.where, schema) else null
    val groupByAccessors = if (stmt.groupBy != null) stmt.groupBy.map(compileColumn[ROW](_, schema)) else null
    val resultSchema = stmtInfo.resultSchema
    val havingExtractor = if (stmt.having != null) compileCondition[Row](stmt.having, resultSchema) else null
    val execPlan = plan("Query") {
      first("Filter the data") {
        val rowBased = table.data.withOption(option)
        val filteredData = if (stmt.where != null) {
          val filterAccessorFunc = RowBasedCompiler.toFunc(filterAccessor)
          rowBased.filter(filterAccessorFunc)
        } else rowBased
        //        println("Filtered data got " + filteredData.asList.mkString("\n"))
        filteredData
      }.next("Grouping the data") {
        filteredData =>
          val groupByIndices = stmtInfo.groupByIndices
          val selectFunc = RowBasedCompiler.toRowFunc(selects)
          //TODO: the detection of aggregate func is problematic when we have multi-project before aggregate function
          val groupedProcessData = if (stmt.groupBy != null) {
            //make sure group columns is in the expanded selects
            val groupByAccessors = stmt.groupBy.map(compileColumn[ROW](_, schema))
            val groupByFunc = (row: ROW) => new Row(groupByAccessors.map(_(row)).toArray)
            val groupedData = filteredData.groupBy(groupByFunc, selectFunc, groupByIndices).map(_.normalize)
            //            println("Grouped Data got " + filteredData)
            val havingData = if (stmt.having != null) {
              val havingFilter = RowBasedCompiler.toFunc(havingExtractor)
              groupedData.filter(havingFilter)
            } else {
              groupedData
            }
            //            println("Having data got " + filteredData.asList.mkString("\n"))
            havingData
          } else if (groupByIndices.size > 0) { //this will trigger group by all
            val selected = filteredData.select(selectFunc)
            selected.reduce((a: Row, b: Row) => a.aggregate(b, groupByIndices)).map(_.normalize)
          } else {
            println("Before filter Option = " + filteredData.option)
            filteredData.select{ selectFunc }
          }
          //          println("Grouped process data got " + groupedProcessData.asList.mkString("\n"))
          groupedProcessData
      }.next("Distinct") {
        groupedProcessData =>
          if (stmt.isDistinct()) {
            groupedProcessData.distinct()
          } else groupedProcessData

      }.next("Ordering the data") {
        groupedProcessData =>
          if (stmt.orderBy != null) {
            val (orderAccessors, ordering) = compileOrdering(stmt.orderBy, resultSchema)
            val keyFunc = (row: Row) => new Row(orderAccessors.map(_(row)).toArray)
            groupedProcessData.sortBy(keyFunc, ordering, scala.reflect.classTag[Row])
          } else {
            groupedProcessData
          }
      }.next("Limit the data") {
        orderedData =>
          if (stmt.limit != null) {
            val (offset, count) = stmt.limit
            //NOTE: we skip the following because for spark we can't know the size without trigger computation
            if (!orderedData.isLazy) {
              if (offset >= orderedData.size) {
                throw new IllegalArgumentException("Offset is more than data length")
              }
              val until = Math.min(orderedData.size, offset + count)
              orderedData.slice(offset, until)
            } else {
              orderedData.slice(offset, offset + count)
            }
          } else orderedData
      }.next("Return the table") {
        groupedProcessData =>
          table.createTable(resultSchema, groupedProcessData)
      }
    }
    execPlan
  }
}


trait AccessorCompiler {

  def table: Table

  val logger = LoggerFactory.getLogger(getClass())
  //Conditions
  def compileCondition[ROW](cond: Condition, schema: Schema): ColumnAccessor[ROW, Boolean] = {
    cond match {
      case bc: BinaryCondition =>
        val a = compileCondition[ROW](bc.a, schema)
        val b = compileCondition[ROW](bc.b, schema)
        new ConditionAccessor[ROW] {
          override def apply(v1: ROW) = bc.evaluate(a(v1), b(v1))
        }
      case ec: EqualityCondition =>
        val a = compileColumn[ROW](ec.a, schema)
        val b = compileColumn[ROW](ec.b, schema)
        new ConditionAccessor[ROW] {
          override def apply(v1: ROW) = ec.evaluate(a(v1), b(v1))
        }
      case nc: NotCondition =>
        val a = compileColumn[ROW](nc.a, schema).asInstanceOf[ColumnAccessor[ROW, Boolean]]
        new ConditionAccessor[ROW] {
          override def apply(v1: ROW) = nc.evaluate(a(v1))
        }
      case _ =>
        throw new IllegalArgumentException("Unknown condition " + cond.toString)
    }
  }

  def compileOrdering(orderSpecs: Seq[OrderSpec], schema: Schema): (Seq[ColumnAccessor[Row, _]], Ordering[Row]) = {
    val accessors = orderSpecs.map(compileColumn[Row](_, schema))
    val ordering = new RowOrdering(orderSpecs.map(_.ascending).toArray)
    (accessors, ordering)
  }

  def compileColumn[ROW](col: Column, schema: Schema, context: String = ""): ColumnAccessor[ROW, _] = {

    val results = col match {
      case mc: MultiColumn =>
        //to handle count(*)
        new ColumnAccessor[ROW, Any] {
          override def apply(v1: ROW) = null
        }
      case cond: Condition =>
        compileCondition[ROW](cond, schema)
      case lc: LiteralColumn[_] =>
        new ColumnAccessor[ROW, Any] {
          override def apply(v1: ROW) = lc.value
        }
      case c: DataColumn =>
        logger.debug("Resolving column " + c.name)
        schema.resolveColumnDef(c.name) match {
          case colRef: ColumnRef => colRef.colDef match {
            case colDef: TypedColumnDef[ROW] =>
              logger.debug("Resolved " + colDef.name)
              //            println("Resolved " + colDef.name + " = " + colDef)
              new ColumnAccessor[ROW, Any] {
                override def apply(v1: ROW): Any = colDef.get(v1)
              }
            case uc =>
              throw new IllegalArgumentException("Unknown column " + c.getResultName + " type " + uc)
          }
          case null =>
            throw new IllegalArgumentException("Unknown column " + c.name)
        }
      case bf: BinaryFunction[Any] =>
        new ColumnAccessor[ROW, Any] {
          val a = compileColumn[ROW](bf.a, schema)
          val b = compileColumn[ROW](bf.b, schema)
          override def apply(v1: ROW) = bf.evaluate(a(v1), b(v1))
        }
      case af: AggregateFunction[_] =>
        val accessors = af.cols.map(compileColumn[ROW](_, schema))
        new ColumnAccessor[ROW, Any] {
          override def apply(v1: ROW) = af.createAggregatable(accessors.map(_(v1)))
        }
      case sq: SubSelect =>
        new ColumnAccessor[ROW, Any] {
          val value: Any = context match {
            case "SELECT" =>
              val info = new StatementInfo(sq.statement.statement(), schema)
              if (info.expandedSelects.size != 1) {
                throw new IllegalArgumentException("Expect single column subquery")
              }
              val results = sq.statement.compile.execute.collectAsList()
              if (results.size != 1) {
                throw new IllegalArgumentException("Expect single result subquery")
              } else {
                results(0).asInstanceOf[Row].data(0)
              }
          }

          override def apply(v1: ROW) = value
        }
      case _ =>
        throw new IllegalArgumentException("Unknown column type " + col)

    }
    results
  }
}

