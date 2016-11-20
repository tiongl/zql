package zql.list

import java.lang.reflect.Field

import zql.core._

import scala.reflect.ClassTag
import scala.util.Try

class ReflectedSchema[T: ClassTag](columnNames: Set[Symbol]) extends Schema[T]{
  val ctag = scala.reflect.classTag[T].runtimeClass

  val columnAccessors = columnNames.map{
    s => (s, getAccessor[T](s.name))
  }.toMap

  private def getAccessor[IN](name: String): ColumnAccessor[IN] = {
    val field = Try(ctag.getField(name)).getOrElse(null)
    if (field!=null){
      new ColumnAccessor[IN](field.getType) {
        def apply(obj: IN) = field.get(obj)
      }
    } else {
      val getter = ctag.getMethod(name)
      if (getter!=null){
        new ColumnAccessor[IN](getter.getReturnType) {
          def apply(obj: IN) = getter.invoke(obj)
        }
      } else {
        throw new IllegalArgumentException("Unknown column " + name + " for type " + ctag)
      }
    }
  }

}