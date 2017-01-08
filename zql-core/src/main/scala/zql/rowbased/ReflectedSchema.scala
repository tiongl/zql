package zql.rowbased

import zql.schema.SimpleSchema
import zql.util.{ LazyMethod, LazyField, Getter }

import scala.reflect.ClassTag
import scala.util.Try

class ReflectedSchema[R: ClassTag](name: String, alias: String = null) extends SimpleSchema(name, alias) {
  @transient implicit val ctag = scala.reflect.classTag[R].runtimeClass

  def resolveGetter(fieldName: String): Getter = {
    val field = Try(ctag.getField(fieldName))
      .getOrElse(null)
    if (field != null) {
      new LazyField(ctag.getCanonicalName, fieldName)
    } else {
      val getter = Try(ctag.getMethod(fieldName))
        .getOrElse(null)
      if (getter != null) {
        new LazyMethod(ctag.getCanonicalName, fieldName)
      } else {
        throw new IllegalArgumentException("Unknown column " + fieldName + " for type " + ctag)
      }
    }
  }

  def i = this

  override def addColumn[T: ClassTag](sym: Symbol) = {
    columns += new ReflectedColumnDef[R](sym, resolveGetter(sym.name))
  }

  def func[B <: R](sym: Symbol, func: (R) => Any) = addColumnDef(new FuncColumnDef[R](sym, func))

}