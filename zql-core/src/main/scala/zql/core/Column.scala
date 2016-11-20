package zql.core

import scala.reflect.ClassTag

/** allow accessing a column from type T **/
abstract class ColumnAccessor[ROW](val dType: Class[_]) extends((ROW) => Any)

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

trait WithAccessor extends Column {
  def getColumnAccessor[ROW](compiler: Table): ColumnAccessor[ROW]
}

abstract class CompositeColumn[T](val cols: Column*) extends TypedColumn[T] with WithAccessor {
  lazy val requiredColumns = cols.map(_.requiredColumns).reduce(_ ++ _)

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

  override def getColumnAccessor[ROW](compiler: Table): ColumnAccessor[ROW] = new ColumnAccessor[ROW](classOf[Boolean]){
    val aa = compiler.compileColumn[ROW](a)
    val bb = compiler.compileColumn[ROW](b)
    def apply(obj: ROW) = aa.apply(obj).asInstanceOf[Boolean] && bb.apply(obj).asInstanceOf[Boolean]
  }
}

class OrCondition(val a: TypedColumn[Boolean], val b: TypedColumn[Boolean]) extends Condition(a, b) {
  def name = Symbol(s"OR(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Table): ColumnAccessor[ROW] = new ColumnAccessor[ROW](classOf[Boolean]){
    val aa = compiler.compileColumn[ROW](a)
    val bb = compiler.compileColumn[ROW](b)
    def apply(obj: ROW) = aa.apply(obj).asInstanceOf[Boolean] || bb.apply(obj).asInstanceOf[Boolean]
  }
}

class Equals(val a: Column, val b: Column) extends Condition(a, b){
  def name = Symbol(s"Eq(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Table): ColumnAccessor[ROW] = new ColumnAccessor[ROW](classOf[Boolean]){
    val aa = compiler.compileColumn[ROW](a)
    val bb = compiler.compileColumn[ROW](b)
    def apply(obj: ROW) = aa.apply(obj) == bb.apply(obj)
  }
}

class NotEquals(val a: Column, val b: Column) extends Condition(a, b){
  def name = Symbol(s"NotEq(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Table): ColumnAccessor[ROW] = new ColumnAccessor[ROW](classOf[Boolean]){
//    val aa = compiler.compileColumn[IN, _](a)
//    val bb = compiler.compileColumn[IN, _](b)
    def apply(obj: ROW) = true//aa.apply(obj) != bb.apply(obj)
  }
}

class NotCondition(val a: TypedColumn[Boolean]) extends Condition(a){
  def name = Symbol(s"Not(${a.getName})")

  override def getColumnAccessor[ROW](compiler: Table): ColumnAccessor[ROW] = new ColumnAccessor[ROW](classOf[Boolean]){
    val aa = compiler.compileColumn[ROW](a)
    def apply(obj: ROW) = !aa.apply(obj).asInstanceOf[Boolean]
  }
}

abstract class Function(cols: Column*) extends CompositeColumn(cols: _*)

abstract class AggregateFunction(cols: Column*) extends Function(cols: _*)

abstract class NamedColumn[T](val name: Symbol) extends TypedColumn[T] {
  def requiredColumns = Set(name)
}

class NumericNamedColumn(n: Symbol) extends NamedColumn[Number](n)

class StringNamedColumn(n: Symbol) extends NamedColumn[String](n)

class GenericNamedColumn(n: Symbol) extends NamedColumn[Any](n)

class BoolNamedColumn(n: Symbol) extends NamedColumn[Boolean](n)

abstract case class LiteralColumn[T](val value: T) extends TypedColumn[T] with WithAccessor {
  def name = Symbol(value.toString)

  def requiredColumns = Set()

  override def getColumnAccessor[ROW](compiler: Table): ColumnAccessor[ROW] = new ColumnAccessor[ROW](classOf[String]){
    override def apply(v1: ROW): Any = value
  }
}

class StringLiteralColumn(value: String) extends LiteralColumn[String](value)

class NumericLiteralColumn(value: Number) extends LiteralColumn[Number](value)

class BooleanLiteralColumn(value: Boolean) extends LiteralColumn[Boolean](value)

abstract class MultiColumn extends Column

class AllColumn extends MultiColumn {
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

class Sum(val col: TypedColumn[Number]) extends AggregateFunction(col) with WithAccessor {
  def name = Symbol(s"SUM(${col.getName})")

  override def getColumnAccessor[ROW](compiler: Table): ColumnAccessor[ROW] = new ColumnAccessor[ROW](classOf[Boolean]){
    val colAccessor = compiler.compileColumn[ROW](col)
    def apply(obj: ROW) = new Summable(colAccessor.apply(obj).asInstanceOf[Int])
  }
}

