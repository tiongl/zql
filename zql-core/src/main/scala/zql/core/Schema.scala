package zql.core

abstract class Schema[T] {
  def columnAccessors(): Map[Symbol, ColumnAccessor[T]]
}

class RowSchema(val columns: Seq[Symbol]) extends Schema[Row] {
  class RowAccessor(i: Int, dType: Class[_]) extends ColumnAccessor[Row](dType) {
    def apply(obj: Row) = obj.data(i)
  }

  val columnAccessors = columns.zipWithIndex.map{
    case (name, i) =>
      (name, new RowAccessor(i, null))
  }.toMap
}
