package zql.core

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

abstract class Schema {

  def allColumns(): Seq[ColumnDef]

  def allColumnNames = allColumns.map(_.name)

  def columnMap: Map[Symbol, ColumnDef] = allColumns().map(c => (c.name, c)).toMap

  def resolveColumnDef(sym: Symbol) = columnMap(sym)
}

abstract class ColumnDef(val name: Symbol) extends Serializable

class SimpleColumnDef(name: Symbol) extends ColumnDef(name)