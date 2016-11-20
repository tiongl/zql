package zql.list

import java.lang.reflect.Field

import zql.core._

import scala.reflect.ClassTag
import scala.util.Try

class ReflectedSchema[ROW: ClassTag](columnNames: Set[Symbol]) extends Schema[ROW]{
  val ctag = scala.reflect.classTag[ROW].runtimeClass

  val columnAccessors = columnNames.map{
    s => (s, getAccessor[ROW](s.name))
  }.toMap

  private def getAccessor[ROW](name: String): ColumnAccessor[ROW, Any] = {
    val field = Try(ctag.getField(name)).getOrElse(null)
    if (field!=null){
      new ColumnAccessor[ROW, Any]() {
        def apply(obj: ROW) = field.get(obj)
      }
    } else {
      val getter = ctag.getMethod(name)
      if (getter!=null){
        new ColumnAccessor[ROW, Any]() {
          def apply(obj: ROW) = getter.invoke(obj)
        }
      } else {
        throw new IllegalArgumentException("Unknown column " + name + " for type " + ctag)
      }
    }
  }

}
