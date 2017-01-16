package zql.rowbased

import zql.core._
import zql.schema.Schema

object StreamingStatementCompiler {
  def toRowFunc[ROW](selects: Seq[ColumnAccessor[ROW, _]]) = (row: ROW) => new Row(selects.map(s => s.apply(row)).toArray)
  def toFunc[ROW, T](accessor: ColumnAccessor[ROW, T]): (ROW) => T = (row: ROW) => accessor.apply(row)
}

class StreamingStatementCompiler[ROW](t: StreamingTable[ROW]) extends RowBasedStatementCompiler[ROW](t.asInstanceOf[RowBasedTable[ROW]]) {
}