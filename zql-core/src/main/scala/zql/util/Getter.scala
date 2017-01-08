package zql.util

abstract class Getter {
  def get(obj: Any): Any
  def dataType: Class[_]
}

//NOTE: lazy field are mainly to accomodate distributed computation because Method/Field are not serializable
case class LazyField(className: String, fieldName: String) extends Getter {
  @transient lazy val field = Class.forName(className).getField(fieldName)
  lazy val dataType = field.getType
  def get(obj: Any) = field.get(obj)
}

case class LazyMethod(className: String, methodName: String) extends Getter {
  @transient lazy val method = {
    val clazz = Class.forName(className)
    if (clazz == null) {
      throw new IllegalStateException("Cannot find class " + className)
    } else {
      clazz.getMethod(methodName)
    }
  }

  lazy val dataType = method.getReturnType

  def get(obj: Any) = method.invoke(obj)
}