package zql.core

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

abstract class Schema {

  def allColumns(): Seq[ColumnDef]

  val allColumnNames = allColumns.map(_.name)

  val columnMap: Map[Symbol, ColumnDef] = allColumns().map(c => (c.name, c)).toMap
}

abstract class ColumnDef(val name: Symbol) extends Serializable
