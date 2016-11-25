package zql.list

import java.lang.reflect.Field
import java.util

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

abstract class Getter {
  def get(obj: Any): Any
}

case class LazyField(className: String, fieldName: String) extends Getter{
  @transient lazy val field = Class.forName(className).getField(fieldName)
  def get(obj: Any) = field.get(obj)
}

case class LazyMethod(className: String, methodName: String) extends Getter {
  @transient lazy val method = Class.forName(className).getMethod(methodName)
  def get(obj: Any) = method.invoke(obj)
}