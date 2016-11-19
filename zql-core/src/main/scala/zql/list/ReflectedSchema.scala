package zql.list

import java.lang.reflect.Field

import zql.core._

import scala.reflect.ClassTag
import scala.util.Try


abstract class ColumnAccessor[T](val dType: Class[_]) extends((T) => Any)

class ReflectedSchema[T: ClassTag](columnNames: Set[Symbol]) extends Schema[T]{
  val ctag = scala.reflect.classTag[T].runtimeClass

  val columnAccessors = columnNames.map{
    s => (s, getAccessor(s.name))
  }.toMap

  private def getAccessor(name: String): ColumnAccessor[T] = {
    val field = Try(ctag.getField(name)).getOrElse(null)
    if (field!=null){
      new ColumnAccessor[T](field.getType) {
        def apply(obj: T) = field.get(obj)
      }
    } else {
      val getter = ctag.getMethod(name)
      if (getter!=null){
        new ColumnAccessor[T](getter.getReturnType) {
          def apply(obj: T) = getter.invoke(obj)
        }
      } else {
        throw new IllegalArgumentException("Unknown column " + name + " for type " + ctag)
      }
    }
  }

}
