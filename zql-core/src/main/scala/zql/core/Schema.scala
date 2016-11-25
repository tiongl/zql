package zql.core

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

abstract class Schema {
//  def columnAccessors(): Map[Symbol, ColumnAccessor[ROW, Any]]
}

abstract class TypedSchema[ROW] extends Schema {
  def allColumns(): Seq[Symbol]
  def getColumnAccessor(name: Symbol): ColumnAccessor[ROW, Any]
}

abstract class Getter {
  def get(obj: Any): Any
}

//NOTE: lazy field are mainly to accomodate distributed computation because Method/Field are not serializable
case class LazyField(className: String, fieldName: String) extends Getter{
  @transient lazy val field = Class.forName(className).getField(fieldName)
  def get(obj: Any) = field.get(obj)
}

case class LazyMethod(className: String, methodName: String) extends Getter {
  @transient lazy val method = Class.forName(className).getMethod(methodName)
  def get(obj: Any) = method.invoke(obj)
}


class ReflectedSchema[ROW: ClassTag](val allColumns: Seq[Symbol]) extends TypedSchema[ROW]{
  val ctag = scala.reflect.classTag[ROW].runtimeClass

  def getColumnAccessor(name: Symbol) = getAccessor(name.name)


  private def getAccessor(name: String): ColumnAccessor[ROW, Any] = {
    val field = Try(ctag.getField(name)).getOrElse(null)
    if (field!=null){
      new ColumnAccessor[ROW, Any]() {
        val lazyField = new LazyField(ctag.getCanonicalName, name)
        def apply(obj: ROW) = lazyField.get(obj)
      }
    } else {
      val getter = ctag.getMethod(name)
      if (getter!=null){
        new ColumnAccessor[ROW, Any]() {
          val lazyMethod = new LazyMethod(ctag.getCanonicalName, name)
          def apply(obj: ROW) = lazyMethod.get(obj)
        }
      } else {
        throw new IllegalArgumentException("Unknown column " + name + " for type " + ctag)
      }
    }
  }


}

case class FuncSchema[T: ClassTag](columnNames: Seq[Symbol], funcs: (Symbol, (T) => Any)*) extends ReflectedSchema[T](columnNames) {
  override val allColumns = {
    val names = mutable.LinkedHashSet(columnNames: _*)
    val moreNames = mutable.LinkedHashSet(funcs.map(_._1): _*)
    val all = names ++ moreNames
    all.toSeq
  }

  private val funcMap = funcs.toMap


  override def getColumnAccessor(name: Symbol): ColumnAccessor[T, Any] = {
    funcMap.get(name) match {
      case None =>
        super.getColumnAccessor(name) //use reflection etc
      case Some(func) =>
        new ColumnAccessor[T, Any] {
          def apply(obj: T) = func(obj)
        }
    }
  }
}

object FuncSchema {
  def create[T: ClassTag](funcs: (Symbol, (T) => Any)*) = {
    new FuncSchema[T](Seq(), funcs: _*)
  }
}





case class RowSchema(val columns: Seq[Symbol]) extends TypedSchema[Row] {
  def allColumns = columns
  class RowAccessor(i: Int) extends ColumnAccessor[Row, Any]() {
    def apply(obj: Row) = obj.data(i)
  }
  override def getColumnAccessor(name: Symbol): ColumnAccessor[Row, Any] = {
    columnAccessors.get(name) match {
      case None =>
        throw new IllegalArgumentException(s"Unknown column ${name}" )
      case Some(a) =>
        a
    }
  }

  val columnAccessors = allColumns.zipWithIndex.map{
    case (name, i) =>
      (name, new RowAccessor(i))
  }.toMap


}
