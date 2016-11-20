package zql.list

import zql.core._

import scala.reflect.ClassTag

/**
  * Created by tiong on 11/17/16.
  */
class ListTable[ROW: ClassTag](val data: List[ROW], schema: Schema[ROW]) extends Table(schema) {

  class DefaultCompiler(schema: Schema[ROW]) extends Compiler[ListTable[_]]{
    def compile(stmt: Statement): Executable[ListTable[_]] = {
      import zql.core.ExecutionPlan._
      val execPlan = plan("Query"){
        first("Filter the data"){
          val filteredData = if (stmt._where!=null){
            val filterExtractor = compileColumn[ROW](stmt._where)
            data.filter(d => filterExtractor(d).asInstanceOf[Boolean])
          } else data
          filteredData
        }.next("Grouping the data") {
          filteredData =>
            val selects = stmt._selects.flatMap(compileSelect[ROW](_))
            //TODO: the detection of aggregate func is problematic when we have multi-project before aggregate function
            val groupByIndices = stmt._selects.zipWithIndex.filter(_._1.isInstanceOf[AggregateFunction[_]]).map(_._2).toArray
            val groupedProcessData = if (stmt._groupBy!=null){
              val groupByExtractor = stmt._groupBy.map(compileColumn(_))
              val groupedList = data.groupBy{
                d => groupByExtractor
              }.map(_._2)
              val groupedData = groupedList.map{
                list => list.map{
                  data => new Row(selects.map(_(data)).toArray)
                }.reduce [Row]{
                  case (a: Row, b: Row) => a.aggregate(b, groupByIndices)
                }
              }
              val havingData = if (stmt._having!=null){
                val havingExtractor = compileColumn[Row](stmt._having)
                groupedData.filter(d => havingExtractor(d).asInstanceOf[Boolean])
              } else {
                groupedData
              }
              havingData
            } else if (groupByIndices.size>0){//this will trigger group by all
              val singleRow = data.map(
                row => new Row(selects.map(_(row)).toArray)
              ).reduce[Row] {
                case (a, b) => a.aggregate(b, groupByIndices)
              }
              val normalized = singleRow.data.map {
                case s: Summable =>
                  s.value
                case any: Any =>
                  any
              }
              List(new Row(normalized))
            }
            else {
              data.map{
                d =>
                  new Row(selects.map(_(d)).toArray)
              }
            }
            groupedProcessData
        }.next("Return the table") {
          groupedProcessData =>
            val names = stmt._selects.map(_.getName)
            new ListTable(groupedProcessData.toList, new RowSchema(names))
        }
      }
      execPlan
    }

    def compileSelect[ROW](col: Column): Seq[ColumnAccessor[ROW, _]] = {
      col match {
        case ac: AllColumn =>
          schema.columnAccessors.map(_._2.asInstanceOf[ColumnAccessor[ROW, _]]).toSeq
        case c: Column =>
          Seq(compileColumn(col))
      }
    }

    def compileColumn[ROW](col: Column): ColumnAccessor[ROW, _] = {
      col match {
        case cc: WithAccessor[_] =>
          cc.getColumnAccessor[ROW](this)
        case c: NamedColumn[_] =>
          schema.columnAccessors()(c.name).asInstanceOf[ColumnAccessor[ROW, _]]
        case _ =>
          throw new IllegalArgumentException("Unknown column type " + col)
      }
    }
  }

  def compile(stmt: Statement) = {
    new DefaultCompiler(schema).compile(stmt)
  }

  override def collectAsList = data
}
