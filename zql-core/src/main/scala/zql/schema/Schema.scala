package zql.schema

import zql.core.ColumnRef

abstract class Schema(val name: String, val alias: String = null) {

  def allColumns(): Seq[ColumnDef]

  def allColumnNames = allColumns.map(_.name)

  def columnMap: Map[Symbol, ColumnDef] = allColumns().map(c => (c.name, c)).toMap

  def as(alias: Symbol) = {
    val newColumns = allColumns.map {
      c =>
        c.rename(Symbol(alias.name + "." + c.name.name))
    }
    new DefaultSchema(name, newColumns, alias.name)
  }

  def resolveColumnDef(colName: Symbol): ColumnRef = {
    //this uses a lot of returns statement with exception at the end to simplify the code
    val prefixes = List(name, alias).filter(_ != null)

    if (columnMap.contains(colName)) {
      return new ColumnRef(this, columnMap(colName))
    } else if (!colName.name.contains(".")) {
      prefixes.foreach {
        prefix =>
          //try with name prefix
          val newColName = Symbol(s"${prefix}.${colName.name}")
          if (columnMap.contains(newColName)) {
            return new ColumnRef(this, columnMap(colName))
          }
      }
    } else {
      val splits = colName.name.split("\\.")
      if (splits.length == 2) {
        val (tbName, colN) = (splits(0), Symbol(splits(1)))
        if (prefixes.contains(tbName)) {
          if (columnMap.contains(colN)) {
            return new ColumnRef(this, columnMap(colN))
          }
        }
      }
    }
    //throw new IllegalArgumentException(s"Column ${colName} not found in [" + allColumnNames.mkString(", ") + "] with prefixes [" + prefixes.mkString(", ") + "]" )
    null
  }
}
