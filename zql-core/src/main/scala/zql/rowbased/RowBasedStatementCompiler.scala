package zql.rowbased

import zql.core._
import zql.schema.Schema

object RowBasedStatementCompiler {
  def toRowFunc[ROW](selects: Seq[ColumnAccessor[ROW, _]]) = (row: ROW) => new Row(selects.map(s => s.apply(row)).toArray)
  def toFunc[ROW, T](accessor: ColumnAccessor[ROW, T]): (ROW) => T = (row: ROW) => accessor.apply(row)
  def aggregateFunc(aggIndices: Array[Int]) = (a: Row, b: Row) => a.aggregate(b, aggIndices)

}

class RowBasedStatementCompiler[ROW](val table: RowBasedTable[ROW]) extends StatementCompiler[Table] with AccessorCompiler {

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
          val filterAccessorFunc = RowBasedStatementCompiler.toFunc(filterAccessor)
          rowBased.filter(filterAccessorFunc)
        } else rowBased
        //        println("Filtered data got " + filteredData.asList.mkString("\n"))
        filteredData
      }.next("Grouping the data") {
        filteredData =>
          val groupByIndices = stmtInfo.groupByIndices
          val selectFunc = RowBasedStatementCompiler.toRowFunc(selects)
          //TODO: the detection of aggregate func is problematic when we have multi-project before aggregate function
          val groupedProcessData = if (stmt.groupBy != null) {
            //make sure group columns is in the expanded selects
            val groupByAccessors = stmt.groupBy.map(compileColumn[ROW](_, schema))
            val keyFunc = (row: ROW) => new Row(groupByAccessors.map(_(row)).toArray)
            val aggFunc = RowBasedStatementCompiler.aggregateFunc(groupByIndices)
            val groupedData = filteredData.groupBy(keyFunc, selectFunc, aggFunc).map(_.normalize)
            //            println("Grouped Data got " + filteredData)
            val havingData = if (stmt.having != null) {
              val havingFilter = RowBasedStatementCompiler.toFunc(havingExtractor)
              groupedData.filter(havingFilter)
            } else {
              groupedData
            }
            //            println("Having data got " + filteredData.asList.mkString("\n"))
            havingData
          } else if (groupByIndices.size > 0) { //this will trigger group by all
            val selected = filteredData.map(selectFunc)
            selected.reduce((a: Row, b: Row) => a.aggregate(b, groupByIndices)).map(_.normalize)
          } else {
            filteredData.map { selectFunc }
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
            groupedProcessData.sortBy(keyFunc, ordering)
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
          table.createTable(resultSchema, groupedProcessData.resultData)
      }
    }
    execPlan
  }
}
