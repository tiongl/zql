package zql.schema

class DefaultSchema(name: String, val allColumns: Seq[ColumnDef], alias: String = null) extends Schema(name, alias) {
  override def toString = "DefaultSchema[" + allColumns.map(_.name.name).mkString(",") + "]"
}
