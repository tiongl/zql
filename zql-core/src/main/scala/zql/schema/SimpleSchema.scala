package zql.schema

import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class SimpleSchema(name: String, alias: String = null) extends Schema(name, alias) {
  protected val columns = new ArrayBuffer[ColumnDef]()

  def allColumns = columns

  def o() = this

  def addColumnDef(colDef: ColumnDef) = columns += colDef

  def addColumn[T: ClassTag](sym: Symbol): Unit = addSimpleColDef[Byte](sym)

  def addSimpleColDef[T: ClassTag](sym: Symbol): Unit = addColumnDef(new SimpleColumnDef(sym, scala.reflect.classTag[T].runtimeClass))

  def addSimpleColDef(sym: Symbol, dataType: Class[_]): Unit = columns += new SimpleColumnDef(sym, dataType)

  def BYTE(sym: Symbol) = addColumn[Byte](sym)

  def SHORT(sym: Symbol) = addColumn[Short](sym)

  def INT(sym: Symbol) = addColumn[Int](sym)

  def LONG(sym: Symbol) = addColumn[Long](sym)

  def FLOAT(sym: Symbol) = addColumn[Float](sym)

  def DOUBLE(sym: Symbol) = addColumn[Double](sym)

  def DATE(sym: Symbol) = addColumn[Date](sym)

  def BOOLEAN(sym: Symbol) = addColumn[Boolean](sym)

  def STRING(sym: Symbol) = addColumn[String](sym)

  def CUSTOM[T: ClassTag](sym: Symbol) = addColumn[T](sym)

}
