package zql.util

import java.lang.reflect.InvocationTargetException

import org.slf4j.LoggerFactory
import zql.core.Column

import scala.reflect.ClassTag
import scala.util.Try

class ColumnVisitor[R <: Any: ClassTag, C <: AnyRef: ClassTag] {
  val logger = LoggerFactory.getLogger("zql.core.ColumnVisitor")
  val rRuntime = scala.reflect.classTag[R].runtimeClass
  val cRuntime = scala.reflect.classTag[C].runtimeClass

  def visit(col: Column, context: C): R = invokeMethodByType(col.getClass, col, context).asInstanceOf[R]

  /**
   * This method seek "handle(col: columnClass, r: R)" method and invoke it. If the method is not found, it will try
   * handle(col: columnClass.getSuperClass, r: R). If there's no hierarchy that matches such type, it will throw an error.
   *
   * @param columnClass
   * @param col
   * @param context
   * @return
   */
  private def invokeMethodByType(columnClass: Class[_], col: Column, context: C): R = {
    logger.debug("Handling " + columnClass)
    val method = Try(getClass.getMethod("handle", columnClass, cRuntime)).getOrElse(null)
    if (method != null) {
      if (!method.getGenericReturnType.toString().equals("R") && !rRuntime.isAssignableFrom(method.getReturnType)) {
        throw new IllegalStateException("Return type of " + method.getReturnType + " does not match " + rRuntime)
      }
      try {
        method.invoke(this, col, context).asInstanceOf[R]
      } catch {
        case ite: InvocationTargetException =>
          //unwrap the exception
          throw ite.getCause
      }
    } else if (columnClass.getSuperclass != null) {
      invokeMethodByType(columnClass.getSuperclass, col, context)
    } else throw new UnsupportedOperationException("Not handling column type " + col.getClass)
  }

  def handle(col: Column, context: C): R = {
    throw new UnsupportedOperationException("This column type " + col.getClass + " is not handled")
  }

}
