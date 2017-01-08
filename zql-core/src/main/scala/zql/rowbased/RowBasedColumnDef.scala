package zql.rowbased

import zql.schema.ColumnDef
import zql.util.Getter

import scala.reflect.ClassTag

abstract class RowBasedColumnDef[R](name: Symbol) extends ColumnDef(name) {
  def get(v1: R): Any
}

class FuncColumnDef[R](name: Symbol, func: (R) => Any) extends RowBasedColumnDef[R](name) {
  override def get(v1: R): Any = func.apply(v1)

  val dataType = classOf[Any]

  override def rename(newName: Symbol): ColumnDef = new FuncColumnDef[R](newName, func)

  def ass(sym: Symbol) = rename(sym)
}

class ReflectedColumnDef[T: ClassTag](name: Symbol, getter: Getter) extends RowBasedColumnDef[T](name) {

  override def get(v1: T): Any = getter.get(v1)

  override def dataType = getter.dataType

  override def rename(newName: Symbol): ColumnDef = new ReflectedColumnDef[T](newName, getter)
}

class RowColumnDef(name: Symbol, i: Int, val dataType: Class[_]) extends RowBasedColumnDef[Row](name) {
  override def get(v1: Row): Any = v1.data(i)
  override def rename(newName: Symbol) = new RowColumnDef(newName, i, dataType)
  override def toString = s"RowColumnDef(${name},${i})"
}
