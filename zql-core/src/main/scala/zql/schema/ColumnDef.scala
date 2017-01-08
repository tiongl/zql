package zql.schema

abstract class ColumnDef(val name: Symbol) extends Serializable {
  def rename(newName: Symbol): ColumnDef
  def dataType: Class[_]
}
