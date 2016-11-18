package zql.core

import scala.reflect.ClassTag

/** a generic column **/
abstract class Column {
  def ===(other: Column): Condition = new Equals(this, other)

  def !==(other: Column): Condition = new NotEquals(this, other)

  def requiredColumns: Set[String]
}

abstract class CompositeColumn(cols: Column*) extends Column {
  lazy val requiredColumns = cols.map(_.requiredColumns).reduce(_ ++ _)
}

//boolean condition
abstract class Condition(cols: Column*) extends CompositeColumn(cols: _*) {
  def and(other: Condition): Condition = new AndCondition(this, other)
  def or(other: Condition): Condition = new OrCondition(this, other)
}

/** a composite condition **/
//TODO: consider support sequence of condition
class AndCondition(val a: Condition, val b: Condition) extends Condition(a, b)

class OrCondition(val a: Condition, val b: Condition) extends Condition(a, b)

class Equals(val a: Column, val b: Column) extends Condition(a, b)

class NotEquals(val a: Column, val b: Column) extends Condition(a, b)

abstract class TypedColumn[T] extends Column

abstract class Function extends Column

abstract class NamedColumn[T](val name: Symbol) extends TypedColumn[T] {
  def requiredColumns = Set(name.name)
}

class NumericNamedColumn(n: Symbol) extends NamedColumn[Number](n)

class StringNamedColumn(n: Symbol) extends NamedColumn[String](n)

class GenericNamedColumn(n: Symbol) extends NamedColumn[Any](n)

abstract case class LiteralColumn[T] extends TypedColumn[T] {
  def requiredColumns = Set()
  def value: Any
}

class StringLiteralColumn(val value: String) extends LiteralColumn[String]

class NumericLiteralColumn(val value: Number) extends LiteralColumn[Number]


class Sum(val column: TypedColumn[Number]) extends Function {
  def requiredColumns = column.requiredColumns
}
