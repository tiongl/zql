package zql.core

abstract class Schema {
//  def columnAccessors(): Map[Symbol, ColumnAccessor[ROW, Any]]
}

abstract class TypedSchema[ROW] extends Schema {
  def columnAccessors(): Map[Symbol, ColumnAccessor[ROW, Any]]
}

case class RowSchema(val columns: Seq[Symbol]) extends TypedSchema[Row] {
  class RowAccessor(i: Int) extends ColumnAccessor[Row, Any]() {
    def apply(obj: Row) = obj.data(i)
  }

  val columnAccessors = columns.zipWithIndex.map{
    case (name, i) =>
      (name, new RowAccessor(i))
  }.toMap
}
