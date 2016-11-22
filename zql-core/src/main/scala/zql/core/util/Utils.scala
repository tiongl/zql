package zql.core.util

import zql.core.Row

import scala.collection.mutable

object Utils {
  def groupBy[T, D](traversable: Traversable[T], keyFunc: (T) => Any, valueFunc: (T) => D, reduceFunc: (D, D) => D): Iterable[D] = {
    val linkedHash = new mutable.LinkedHashMap[Any, D]
    val results = traversable.foreach{
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
}
