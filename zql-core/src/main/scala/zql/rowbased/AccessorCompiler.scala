package zql.rowbased

import org.slf4j.LoggerFactory
import zql.core._
import zql.schema.Schema

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
            case colDef: RowBasedColumnDef[ROW] =>
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
