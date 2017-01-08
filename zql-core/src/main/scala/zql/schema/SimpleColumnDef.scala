package zql.schema

class SimpleColumnDef(name: Symbol, val dataType: Class[_]) extends ColumnDef(name) {
  def rename(newName: Symbol) = new SimpleColumnDef(newName, dataType)
}
