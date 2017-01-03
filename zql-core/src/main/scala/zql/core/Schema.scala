package zql.core

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

abstract class Schema {

  def allColumns(): Seq[ColumnDef]

  def allColumnNames = allColumns.map(_.name)

  def columnMap: Map[Symbol, ColumnDef] = allColumns().map(c => (c.name, c)).toMap

  def resolveColumnDef(colName: Symbol, prefixes: List[String] = List()): ColumnDef = {
    //this uses a lot of returns statement with exception at the end to simplify the code
    if (columnMap.contains(colName)) {
      return columnMap(colName)
    } else if (!colName.name.contains(".")) {
      prefixes.foreach {
        prefix =>
          //try with name prefix
          val newColName = Symbol(s"${prefix}.${colName.name}")
          if (columnMap.contains(newColName)) {
            return columnMap(colName)
          }
      }
    } else {
      val splits = colName.name.split("\\.")
      if (splits.length == 2) {
        val (tbName, colN) = (splits(0), Symbol(splits(1)))
        if (prefixes.contains(tbName)) {
          if (columnMap.contains(colN)) {
            return columnMap(colN)
          }
        }
      }
    }
    throw new IllegalArgumentException(s"Column ${colName} not found")
  }
}

class JoinedSchema(tb1: Table, tb2: Table) extends Schema {
  val logger = LoggerFactory.getLogger(classOf[JoinedSchema])

  override def allColumns(): Seq[ColumnDef] = {
    val tb1Cols = tb1.schema.allColumns().map(col => col.rename(Symbol(tb1.getPrefixes().last + "." + col.name.name)))
    val tb2Cols = tb2.schema.allColumns().map(col => col.rename(Symbol(tb2.getPrefixes().last + "." + col.name.name)))
    tb1Cols ++ tb2Cols
  }
  logger.warn("all columns = " + allColumns().map(_.name).mkString(", "))
}

abstract class ColumnDef(val name: Symbol) extends Serializable {
  def rename(newName: Symbol): ColumnDef
}

class SimpleColumnDef(name: Symbol) extends ColumnDef(name) {
  def rename(newName: Symbol) = new SimpleColumnDef(newName)
}