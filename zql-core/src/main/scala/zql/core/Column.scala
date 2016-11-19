package zql.core

import zql.list.ColumnAccessor

import scala.reflect.ClassTag

/** a generic column **/
abstract class Column {

  var alias: Symbol = null

  def name: Symbol

  def as(name: Symbol) = { alias = name }

  def getName = if (alias==null) name else alias

  def ===(other: Column): Condition = new Equals(this, other)

  def !==(other: Column): Condition = new NotEquals(this, other)

  def requiredColumns: Set[Symbol]
}

abstract class TypedColumn[T] extends Column

abstract class CompositeColumn[T](val cols: Column*) extends TypedColumn[T] {
  lazy val requiredColumns = cols.map(_.requiredColumns).reduce(_ ++ _)

  def getColumnAccessor[T](compiler: Table): ColumnAccessor[T]
}

//boolean condition
abstract class Condition(cols: Column*) extends CompositeColumn[Boolean](cols: _*) {
  def and(other: Condition): Condition = new AndCondition(this, other)
  def or(other: Condition): Condition = new OrCondition(this, other)

}

/** a composite condition **/
//TODO: consider support sequence of condition
class AndCondition(val a: TypedColumn[Boolean], val b: TypedColumn[Boolean]) extends Condition(a, b) {
  def name = Symbol(s"AND(${a.getName},${b.getName})")

  override def getColumnAccessor[T](compiler: Table): ColumnAccessor[T] = new ColumnAccessor[T](classOf[Boolean]){
    val aa = compiler.compileColumn[T](a)
    val bb = compiler.compileColumn[T](b)
    def apply(obj: T) = aa.apply(obj).asInstanceOf[Boolean] && bb.apply(obj).asInstanceOf[Boolean]
  }
}

class OrCondition(val a: TypedColumn[Boolean], val b: TypedColumn[Boolean]) extends Condition(a, b) {
  def name = Symbol(s"OR(${a.getName},${b.getName})")

  override def getColumnAccessor[T](compiler: Table): ColumnAccessor[T] = new ColumnAccessor[T](classOf[Boolean]){
    val aa = compiler.compileColumn[T](a)
    val bb = compiler.compileColumn[T](b)
    def apply(obj: T) = aa.apply(obj).asInstanceOf[Boolean] || bb.apply(obj).asInstanceOf[Boolean]
  }
}

class Equals(val a: Column, val b: Column) extends Condition(a, b){
  def name = Symbol(s"Eq(${a.getName},${b.getName})")

  override def getColumnAccessor[T](compiler: Table): ColumnAccessor[T] = new ColumnAccessor[T](classOf[Boolean]){
    val aa = compiler.compileColumn[T](a)
    val bb = compiler.compileColumn[T](b)
    def apply(obj: T) = aa.apply(obj) == bb.apply(obj)
  }
}

class NotEquals(val a: Column, val b: Column) extends Condition(a, b){
  def name = Symbol(s"NotEq(${a.getName},${b.getName})")

  override def getColumnAccessor[T](compiler: Table): ColumnAccessor[T] = new ColumnAccessor[T](classOf[Boolean]){
    val aa = compiler.compileColumn[T](a)
    val bb = compiler.compileColumn[T](b)
    def apply(obj: T) = aa.apply(obj) != bb.apply(obj)
  }
}

class NotCondition(val a: TypedColumn[Boolean]) extends Condition(a){
  def name = Symbol(s"Not(${a.getName})")

  override def getColumnAccessor[T](compiler: Table): ColumnAccessor[T] = new ColumnAccessor[T](classOf[Boolean]){
    val aa = compiler.compileColumn[T](a)
    def apply(obj: T) = !aa.apply(obj).asInstanceOf[Boolean]
  }
}

abstract class Function[T](cols: Column*) extends CompositeColumn[T](cols: _*)

abstract class AggregateFunction[T](cols: Column*) extends Function[T](cols: _*)

abstract class NamedColumn[T](val name: Symbol) extends TypedColumn[T] {
  def requiredColumns = Set(name)
}

class NumericNamedColumn(n: Symbol) extends NamedColumn[Number](n)

class StringNamedColumn(n: Symbol) extends NamedColumn[String](n)

class GenericNamedColumn(n: Symbol) extends NamedColumn[Any](n)

class BoolNamedColumn(n: Symbol) extends NamedColumn[Boolean](n)

abstract case class LiteralColumn[T] extends TypedColumn[T] {
  def requiredColumns = Set()
  def value: Any
}

class StringLiteralColumn(val value: String) extends LiteralColumn[String] {
  def name = Symbol(value.toString)
}

class NumericLiteralColumn(val value: Number) extends LiteralColumn[Number] {
  def name = Symbol(value.toString)
}

abstract class MultiProjectColumn extends Column

class AllColumn extends Column {
  def requiredColumns = Set()
  def name = Symbol("*")
}


abstract class Aggregatable {
  def aggregate(agg: Aggregatable): Aggregatable
}

class Summable(val n: Number) extends Aggregatable {
  var value = n
  override def aggregate(agg: Aggregatable) = {
    new Summable(value.intValue + agg.asInstanceOf[Summable].n.intValue())
  }

  override def toString = value.toString
}

class Sum(val col: TypedColumn[Number]) extends AggregateFunction(col) {
  def name = Symbol(s"SUM(${col.getName})")

  override def getColumnAccessor[T](compiler: Table): ColumnAccessor[T] = new ColumnAccessor[T](classOf[Boolean]){
    println(col)
    val aa = compiler.compileColumn[T](col)
    def apply(obj: T) = new Summable(aa.apply(obj).asInstanceOf[Int])
  }
}

