package zql.core

import scala.reflect.ClassTag
import scala.util.Try


abstract class ValueGetter[T]  {
  def get(obj: T): Any
}

abstract class Aggregatable {
  def aggregate(agg: Aggregatable): Aggregatable
}

abstract class Aggregator[T] extends ValueGetter[T] {
  def get(obj: T): Aggregatable
}

class ReflectionGetter[T: ClassTag](name: String) extends ValueGetter[T]{
  val ctag = scala.reflect.classTag[T].runtimeClass
  val field = Try(ctag.getField(name)).getOrElse(null)
  val getter = ctag.getMethod(name)
  if (field==null && getter==null){
    throw new IllegalArgumentException("Unknown column " + name + " for type " + ctag)
  }

  override def get(obj: T) = {
    if (field!=null){
      field.get(obj)
    } else {
      getter.invoke(obj)
    }
  }

  //  override def aggregate(a: T, b: T): T = ???
}

class LiteralGetter[T](value: Any) extends ValueGetter[T]{
  override def get(obj: T) = value
}

class AndGetter[T](a: ValueGetter[T], b: ValueGetter[T]) extends ValueGetter[T]{
  override def get(obj: T) = a.get(obj).asInstanceOf[Boolean] && b.get(obj).asInstanceOf[Boolean]
}

class OrGetter[T](a: ValueGetter[T], b: ValueGetter[T]) extends ValueGetter[T]{
  override def get(obj: T) = a.get(obj).asInstanceOf[Boolean] || b.get(obj).asInstanceOf[Boolean]
}

class SumGetter[T](a: ValueGetter[T]) extends Aggregator[T]{

  class Summable(val n: Number) extends Aggregatable {
    var value = n
    override def aggregate(agg: Aggregatable) = {
      new Summable(value.intValue + agg.asInstanceOf[Summable].n.intValue())
    }

    override def toString = value.toString
  }
  override def get(obj: T) = new Summable(a.get(obj).asInstanceOf[Number])
}


//
//
//abstract class Aggregatable[E] extends Extractor[E] {
//  def aggregate(a: E, b: E): E
//}
//
//class LiteralExtrator[T](value: T) extends Extractor[T]{
//
//  override def extractValue(row: Any): T = value
//
//  //  override def aggregate(a: T, b: T): T = ???
//}
/**
  * Created by tiong on 11/17/16.
  */
object ValueGetter {
  type ValueGetter[T] = (T)
}
