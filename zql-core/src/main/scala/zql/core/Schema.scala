package zql.core

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

abstract class Schema {

  def allColumns(): Seq[ColumnDef]

  def allColumnNames = allColumns.map(_.name)

  def columnMap: Map[Symbol, ColumnDef] = allColumns().map(c => (c.name, c)).toMap

  def resolveColumnDef(sym: Symbol) = columnMap(sym)
}

class JoinedSchema(tb1: Table, tb2: Table) extends Schema {
  val logger = LoggerFactory.getLogger(classOf[JoinedSchema])
  override def allColumns(): Seq[ColumnDef] = tb1.schema.allColumns() ++ tb2.schema.allColumns
  logger.warn("all columns = " + allColumns().map(_.name).mkString(", "))
}

abstract class ColumnDef(val name: Symbol) extends Serializable {
  def rename(newName: Symbol): ColumnDef
}

class SimpleColumnDef(name: Symbol) extends ColumnDef(name) {
  def rename(newName: Symbol) = new SimpleColumnDef(newName)
}