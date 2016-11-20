package zql.core

abstract class Schema[ROW] {
  def columnAccessors(): Map[Symbol, ColumnAccessor[ROW, Any]]
}

class RowSchema(val columns: Seq[Symbol]) extends Schema[Row] {
  class RowAccessor(i: Int) extends ColumnAccessor[Row, Any]() {
    def apply(obj: Row) = obj.data(i)
  }

  val columnAccessors = columns.zipWithIndex.map{
    case (name, i) =>
      (name, new RowAccessor(i))
  }.toMap
}
