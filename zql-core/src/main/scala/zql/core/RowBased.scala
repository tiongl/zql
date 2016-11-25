package zql.core

import zql.core.util.Utils.SeqOrdering

import scala.reflect.ClassTag

trait RowBased[ROW] {
  def slice(offset: Int, until: Int): RowBased[ROW]
  def select(r: (ROW) => Row): RowBased[Row]
  def filter(filter: (ROW) => Boolean): RowBased[ROW]
  def groupBy(rowBased: RowBased[ROW], keyFunc: (ROW) => Seq[Any], valueFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBased[Row]
  def reduce(reduceFunc: (ROW, ROW) => ROW): RowBased[ROW]
  def map(mapFunc: (ROW) => Row): RowBased[Row]
  def sortBy[K](keyFunc: (ROW) => K, ordering: Ordering[K], tag: ClassTag[K]): RowBased[ROW]
  def size: Int
  def asList: List[ROW]
}

abstract class RowBasedTable[T](schema: TypedSchema[T]) extends TypedTable[T](schema){

  def data: RowBased[T]

  override def compile(stmt: Statement): Executable[Table] = {
    new RowBasedCompiler(this, schema).compile(stmt)
  }

  def createTable[T: ClassTag](rowBased: RowBased[T], newShema: TypedSchema[T]): RowBasedTable[T]

  def collectAsList() = data.asList

}

class RowBasedCompiler[ROW](table: RowBasedTable[ROW], schema: TypedSchema[ROW]) extends Compiler[RowBasedTable[Row]] {

    def validate(stmt: Statement) = {
      if (stmt._groupBy!=null){
        val aggFunctions = stmt._selects.filter(_.isInstanceOf[AggregateFunction[_]])
        if (aggFunctions.size==0) {
          throw new IllegalArgumentException("Group by must have at least one aggregation function")
        }
      }
    }

    def compile(stmt: Statement): Executable[RowBasedTable[Row]] = {
      import zql.core.ExecutionPlan._
      validate(stmt)

      val selectMappings = compileSelects(stmt._selects, schema)
      val newColumns = selectMappings.map(_._1)
      val resultSchema = new RowSchema(newColumns)
      val rowBased = table.data
      val execPlan = plan("Query"){
        first("Filter the data"){
          val filteredData = if (stmt._where!=null){
            val filterAccesor = compileColumn[ROW](stmt._where, schema).asInstanceOf[ColumnAccessor[ROW, Boolean]]
            rowBased.filter((row: ROW) => filterAccesor(row))
          } else rowBased
          filteredData
        }.next("Grouping the data") {
          filteredData =>
            val selects = selectMappings.map(_._2)
            val selectFunc = (row: ROW) => new Row(selects.map(_(row)).toArray)
            //TODO: the detection of aggregate func is problematic when we have multi-project before aggregate function
            val groupByIndices = stmt._selects.zipWithIndex.filter(_._1.isInstanceOf[AggregateFunction[_]]).map(_._2).toArray
            val groupedProcessData = if (stmt._groupBy!=null){
              val groupByAccessors = stmt._groupBy.map(compileColumn[ROW](_, schema))
              val groupByFunc = (row: ROW) => groupByAccessors.map(_(row))
              val groupedData = filteredData.groupBy(filteredData, groupByFunc, selectFunc, groupByIndices).map(_.normalize)

              val havingData = if (stmt._having!=null){
                val havingExtractor = compileColumn[Row](stmt._having, resultSchema)
                val havingFilter = (row: Row) => havingExtractor(row).asInstanceOf[Boolean]
                groupedData.filter(havingFilter)
              } else {
                groupedData
              }
              havingData
            } else if (groupByIndices.size>0){//this will trigger group by all
              val selected = filteredData.select(selectFunc)
              selected.reduce((a: Row, b: Row) => a.aggregate(b, groupByIndices)).map(_.normalize)
            } else {
              filteredData.map{ selectFunc }
            }
            groupedProcessData
        }.next("Ordering the data") {
          groupedProcessData => if (stmt._orderBy!=null){
            val (orderAccessors, ordering) = compileOrdering(stmt._orderBy, resultSchema)
            val keyFunc = (row: Row) => orderAccessors.map(_(row))
            groupedProcessData.sortBy(keyFunc, ordering, scala.reflect.classTag[Seq[Any]])
          } else {
            groupedProcessData
          }
        }.next("Limit the data") {
          orderedData => if (stmt._limit!=null) {
            val (offset, count) = stmt._limit
            //NOTE: we skip the following because for spark we can't know the size without trigger computation
            if (offset>=orderedData.size){
              throw new IllegalArgumentException("Offset is more than data length")
            }
            val until = Math.min(orderedData.size, offset + count)
            orderedData.slice(offset, until)
          } else orderedData
        }.next("Return the table") {
          groupedProcessData =>
            table.createTable(groupedProcessData, resultSchema)
        }
      }
      execPlan
    }

    def compileSelects[ROW](selects: Seq[Column], schema: TypedSchema[ROW]): Seq[(Symbol, ColumnAccessor[ROW, _])] = {
      selects.flatMap {
        case ac: MultiColumn =>
          ac.toColumns(schema).map(col =>
            (col.name, compileColumn[ROW](col, schema)))
        case c: Column =>
          Seq((c.getName, compileColumn[ROW](c, schema)))
      }
    }

    def compileOrdering(orderSpecs: Seq[OrderSpec], schema: TypedSchema[Row]): (Seq[ColumnAccessor[Row, _]], Ordering[Seq[Any]]) = {
      val accessors = orderSpecs.map(compileColumn[Row](_, schema))
      val ordering = new SeqOrdering(orderSpecs.map(_.ascending).toArray)
      (accessors, ordering)
    }

    def compileColumn[ROW](col: Column, schema: TypedSchema[ROW]): ColumnAccessor[ROW, _] = {
      col match {
        case cc: WithAccessor[_] =>
          cc.getColumnAccessor[ROW](this, schema)
        case c: NamedColumn[_] =>
          schema.getColumnAccessor(c.name)
        case _ =>
          throw new IllegalArgumentException("Unknown column type " + col)
      }
    }

}
