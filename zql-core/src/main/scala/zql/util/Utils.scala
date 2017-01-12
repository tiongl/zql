package zql.util

import scala.collection.mutable

object Utils {
  def groupBy[T, D](traversable: Traversable[T], keyFunc: (T) => Any, valueFunc: (T) => D, reduceFunc: (D, D) => D): Iterable[D] = {
    val linkedHash = new mutable.LinkedHashMap[Any, D]
    val results = traversable.foreach {
      data =>
        val key = keyFunc.apply(data)
        val value = valueFunc.apply(data)
        linkedHash.get(key) match {
          case None =>
            linkedHash.put(key, value)
          case Some(current) =>
            linkedHash.put(key, reduceFunc(current, value))
        }
    }
    linkedHash.values
  }

  def compare(a: Any, b: Any): Int = {
    (a, b) match {
      case (ai: Int, bi: Int) =>
        Ordering.Int.compare(ai, bi)
      case (al: Long, bl: Long) =>
        Ordering.Long.compare(al, bl)
      case (af: Float, bf: Float) =>
        Ordering.Float.compare(af, bf)
      case (ad: Double, bd: Float) =>
        Ordering.Double.compare(ad, bd)
      case (as: String, bs: String) =>
        Ordering.String.compare(as, bs)
      case (as: Boolean, bs: Boolean) =>
        Ordering.Boolean.compare(as, bs)
      case _ =>
        throw new IllegalArgumentException("Unknown type for comparison " + a + " " + b)
    }
  }

  def <(a: Any, b: Any): Boolean = {
    compare(a, b) == -1
  }

  def <=(a: Any, b: Any): Boolean = {
    val compared = compare(a, b)
    compared == -1 || compared == 0
  }

  def >(a: Any, b: Any): Boolean = {
    compare(a, b) == 1
  }

  def >=(a: Any, b: Any): Boolean = {
    val compared = compare(a, b)
    compared == 1 || compared == 0
  }

}
