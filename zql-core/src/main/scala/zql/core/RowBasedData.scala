package zql.core

import java.util.UUID

import zql.core.util.Utils.SeqOrdering

import scala.reflect.ClassTag
import scala.util.Try

trait RowBasedData[ROW] {
  def withOption(option: CompileOption): RowBasedData[ROW]
  def slice(offset: Int, until: Int): RowBasedData[ROW]
  def select(r: (ROW) => Row): RowBasedData[Row]
  def filter(filter: (ROW) => Boolean): RowBasedData[ROW]
  def groupBy(keyFunc: (ROW) => Seq[Any], valueFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row]
  def reduce(reduceFunc: (ROW, ROW) => ROW): RowBasedData[ROW]
  def map(mapFunc: (ROW) => Row): RowBasedData[Row]
  def sortBy[K](keyFunc: (ROW) => K, ordering: Ordering[K], tag: ClassTag[K]): RowBasedData[ROW]
  def size: Int
  def asList: List[ROW]
  def isLazy: Boolean
  def distinct(): RowBasedData[ROW]
}

class DefaultSchema(val allColumns: ColumnDef*) extends Schema

abstract class Getter {
  def get(obj: Any): Any
}

//NOTE: lazy field are mainly to accomodate distributed computation because Method/Field are not serializable
case class LazyField(className: String, fieldName: String) extends Getter {
  @transient lazy val field = Class.forName(className).getField(fieldName)
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
  def get(obj: Any) = method.invoke(obj)
}

abstract class TypedColumnDef[T](name: Symbol) extends ColumnDef(name) with ((T) => Any)

class RowColumnDef(name: Symbol, i: Int) extends TypedColumnDef[Row](name) {
  override def apply(v1: Row): Any = v1.data(i)
}

class ReflectionColumnDef[T: ClassTag](name: Symbol) extends TypedColumnDef[T](name) {

  @transient implicit val ctag = scala.reflect.classTag[T].runtimeClass
  val getter: Getter = {
    val fieldName = name.name
    val field = Try(ctag.getField(fieldName)).getOrElse(null)
    if (field != null) {
      new LazyField(ctag.getCanonicalName, fieldName)
    } else {
      val getter = ctag.getMethod(fieldName)
      if (getter != null) {
        new LazyMethod(ctag.getCanonicalName, fieldName)
      } else {
        throw new IllegalArgumentException("Unknown column " + fieldName + " for type " + ctag)
      }
    }
  }

  override def apply(v1: T): Any = getter.get(v1)
}

abstract class RowBasedTable[R](val schema: Schema,
                                val name: String = getClass.getSimpleName) extends Table {
  def id = UUID.randomUUID().toString

  override def toString = name + id

  def data: RowBasedData[R]

  override def compile(stmt: Statement): Executable[Table] = {
    getCompiler.compile(stmt, schema)
  }

  def createTable[T: ClassTag](rowBased: RowBasedData[T], newShema: DefaultSchema): RowBasedTable[T]

  def collectAsList() = data.asList

  def getCompiler[TB <: Table]() = new RowBasedCompiler[R](this).asInstanceOf[Compiler[TB]]

}

/** allow accessing a column from type T **/
abstract class ColumnAccessor[ROW, +T]() extends ((ROW) => T) with Serializable

abstract class ConditionAccessor[ROW]() extends ColumnAccessor[ROW, Boolean]

class RowBasedCompiler[ROW](table: RowBasedTable[ROW]) extends Compiler[RowBasedTable[Row]] {

  case class StatementInfo(stmt: Statement, schema: Schema) {
    val expandedSelects = stmt.select.flatMap {
      case mc: MultiColumn =>
        mc.toColumns(schema)
      case c => Seq(c)
    }

    val groupByIndices = expandedSelects.zipWithIndex.filter(_._1.isInstanceOf[AggregateFunction[_]]).map(_._2).toArray
    val newColumns = expandedSelects.zipWithIndex.map {
      case (col, index) => new RowColumnDef(col.getResultName, index)
    }
    val resultSchema = new DefaultSchema(newColumns: _*)
    val selects = expandedSelects.map(c => compileColumn[ROW](c, schema, "SELECT"))
    val filterAccesor: ColumnAccessor[ROW, Boolean] = if (stmt.where != null) compileCondition[ROW](stmt.where, schema) else null
    val groupByAccessors = if (stmt.groupBy != null) stmt.groupBy.map(compileColumn[ROW](_, schema)) else null
    val havingExtractor = if (stmt.having != null) compileCondition[Row](stmt.having, resultSchema) else null
  }

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
    val resultSchema = stmtInfo.resultSchema
    val execPlan = plan("Query") {
      first("Filter the data") {
        val rowBased = table.data.withOption(option)
        val filteredData = if (stmt.where != null) {
          val filterAccessor = stmtInfo.filterAccesor
          rowBased.withOption(option).filter((row: ROW) => filterAccessor(row))
        } else rowBased
        filteredData
      }.next("Grouping the data") {
        filteredData =>
          val selects = stmtInfo.selects //to avoid spark serialization issue
          val groupByIndices = stmtInfo.groupByIndices

          val selectFunc = (row: ROW) => new Row(selects.map(_(row)).toArray)
          //TODO: the detection of aggregate func is problematic when we have multi-project before aggregate function

          val groupedProcessData = if (stmt.groupBy != null) {
            //make sure group columns is in the expanded selects
            val groupByAccessors = stmt.groupBy.map(compileColumn[ROW](_, schema))
            val groupByFunc = (row: ROW) => groupByAccessors.map(_(row))
            val groupedData = filteredData.groupBy(groupByFunc, selectFunc, groupByIndices).map(_.normalize)

            val havingData = if (stmt.having != null) {
              val havingExtractor = stmtInfo.havingExtractor
              val havingFilter = (row: Row) => havingExtractor(row)
              groupedData.filter(havingFilter)
            } else {
              groupedData
            }
            havingData
          } else if (groupByIndices.size > 0) { //this will trigger group by all
            val selected = filteredData.select(selectFunc)
            selected.reduce((a: Row, b: Row) => a.aggregate(b, groupByIndices)).map(_.normalize)
          } else {
            filteredData.map { selectFunc }
          }
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
            val keyFunc = (row: Row) => orderAccessors.map(_(row))
            groupedProcessData.sortBy(keyFunc, ordering, scala.reflect.classTag[Seq[Any]])
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
          table.createTable(groupedProcessData, resultSchema)
      }
    }
    execPlan
  }

  def compileSelects[ROW](selects: Seq[Column], schema: DefaultSchema): Seq[(Symbol, ColumnAccessor[ROW, _])] = {
    selects.flatMap {
      case ac: MultiColumn =>
        ac.toColumns(schema).map(col =>
          (col.name, compileColumn[ROW](col, schema)))
      case c: Column =>
        Seq((c.getResultName, compileColumn[ROW](c, schema, "SELECT")))
    }
  }

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

  def compileOrdering(orderSpecs: Seq[OrderSpec], schema: DefaultSchema): (Seq[ColumnAccessor[Row, _]], Ordering[Seq[Any]]) = {
    val accessors = orderSpecs.map(compileColumn[Row](_, schema))
    val ordering = new SeqOrdering(orderSpecs.map(_.ascending).toArray)
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
      case c: DataColumn[_] =>
        schema.resolveColumnDef(c.name) match {
          case colDef: TypedColumnDef[ROW] =>
            new ColumnAccessor[ROW, Any] {
              override def apply(v1: ROW): Any = colDef(v1)
            }
          case uc =>
            throw new IllegalArgumentException("Unknown column " + c.getResultName + " type " + uc)
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
