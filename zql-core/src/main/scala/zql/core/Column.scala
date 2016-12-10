package zql.core

import zql.core.util.Utils

/** a generic column **/
abstract class Column extends Serializable {

  var alias: Symbol = null

  def name: Symbol

  override def toString = name.name

  def as(name: Symbol) = {
    alias = name
    this
  }

  def castToNumeric: NumericColumn = ???

  def getResultName = if (alias == null) name else alias

  def ===(other: Column): Condition = new Equals(this, other)

  def !==(other: Column): Condition = new NotEquals(this, other)

  def <(other: Column): Condition = new LessThan(castToNumeric, other.castToNumeric)

  def <=(other: Column): Condition = new LessThanEquals(castToNumeric, other.castToNumeric)

  def >(other: Column): Condition = new GreaterThan(castToNumeric, other.castToNumeric)

  def >=(other: Column): Condition = new GreaterThanEquals(castToNumeric, other.castToNumeric)

  def +(other: Column): Function[Any] = new Plus(this, other)

  def -(other: Column): NumericColumn = new Minus(castToNumeric, other.castToNumeric)

  def *(other: Column): NumericColumn = new Multiply(castToNumeric, other.castToNumeric)

  def /(other: Column): NumericColumn = new Divide(castToNumeric, other.castToNumeric)

  def requiredColumns: Set[Symbol]
}

/** column with a type **/
abstract class TypedColumn[T] extends Column

/** just a tagging interface for numeric column **/
trait NumericColumn extends Column

//TODO: Improve the implementations of math operations
object NumericColumn {

  def +(a: Any, b: Any): Any = a match {
    case (c: Double) => b match {
      case (d: Double) =>
        c + d
      case (d: Float) =>
        c + d
      case (d: Short) =>
        c + d
      case (d: Byte) =>
        c + d
      case (d: Int) =>
        c + d
      case _ =>
        throw new IllegalArgumentException("Plus operation on unknonw type " + b.getClass)

    }
    case (c: Float) => b match {
      case (d: Double) =>
        c + d
      case (d: Float) =>
        c + d
      case (d: Short) =>
        c + d
      case (d: Byte) =>
        c + d
      case (d: Int) =>
        c + d
      case _ =>
        throw new IllegalArgumentException("Plus operation on unknonw type " + b.getClass)
    }
    case (c: Short) => b match {
      case (d: Double) =>
        c + d
      case (d: Float) =>
        c + d
      case (d: Short) =>
        c + d
      case (d: Byte) =>
        c + d
      case (d: Int) =>
        c + d
      case _ =>
        throw new IllegalArgumentException("Plus operation on unknonw type " + b.getClass)
    }
    case (c: Int) => b match {
      case (d: Double) =>
        c + d
      case (d: Float) =>
        c + d
      case (d: Short) =>
        c + d
      case (d: Byte) =>
        c + d
      case (d: Int) =>
        c + d
      case _ =>
        throw new IllegalArgumentException("Plus operation on unknonw type " + b.getClass)
    }
    case (c: Byte) => b match {
      case (d: Double) =>
        c + d
      case (d: Float) =>
        c + d
      case (d: Short) =>
        c + d
      case (d: Byte) =>
        c + d
      case (d: Int) =>
        c + d
      case _ =>
        throw new IllegalArgumentException("Plus operation on unknonw type " + b.getClass)
    }
    case _ =>
      throw new IllegalArgumentException("Plus operation on unknonw type " + a.getClass)
  }

  def -(a: Any, b: Any): Any = a match {
    case (c: Double) => b match {
      case (d: Double) =>
        c - d
      case (d: Float) =>
        c - d
      case (d: Short) =>
        c - d
      case (d: Byte) =>
        c - d
      case (d: Int) =>
        c - d
      case _ =>
        throw new IllegalArgumentException("Minus operation on unknonw type " + b.getClass)

    }
    case (c: Float) => b match {
      case (d: Double) =>
        c - d
      case (d: Float) =>
        c - d
      case (d: Short) =>
        c - d
      case (d: Byte) =>
        c - d
      case (d: Int) =>
        c - d
      case _ =>
        throw new IllegalArgumentException("Minus operation on unknonw type " + b.getClass)
    }
    case (c: Short) => b match {
      case (d: Double) =>
        c - d
      case (d: Float) =>
        c - d
      case (d: Short) =>
        c - d
      case (d: Byte) =>
        c - d
      case (d: Int) =>
        c - d
      case _ =>
        throw new IllegalArgumentException("Minus operation on unknonw type " + b.getClass)
    }
    case (c: Int) => b match {
      case (d: Double) =>
        c - d
      case (d: Float) =>
        c - d
      case (d: Short) =>
        c - d
      case (d: Byte) =>
        c - d
      case (d: Int) =>
        c - d
      case _ =>
        throw new IllegalArgumentException("Minus operation on unknonw type " + b.getClass)
    }
    case (c: Byte) => b match {
      case (d: Double) =>
        c - d
      case (d: Float) =>
        c - d
      case (d: Short) =>
        c - d
      case (d: Byte) =>
        c - d
      case (d: Int) =>
        c - d
      case _ =>
        throw new IllegalArgumentException("Minus operation on unknonw type " + b.getClass)
    }
    case _ =>
      throw new IllegalArgumentException("Minus operation on unknonw type " + a.getClass)
  }

  def /(a: Any, b: Any): Any = a match {
    case (c: Double) => b match {
      case (d: Double) =>
        c / d
      case (d: Float) =>
        c / d
      case (d: Short) =>
        c / d
      case (d: Byte) =>
        c / d
      case (d: Int) =>
        c / d
      case _ =>
        throw new IllegalArgumentException("Divide operation on unknonw type " + b.getClass)

    }
    case (c: Float) => b match {
      case (d: Double) =>
        c / d
      case (d: Float) =>
        c / d
      case (d: Short) =>
        c / d
      case (d: Byte) =>
        c / d
      case (d: Int) =>
        c / d
      case _ =>
        throw new IllegalArgumentException("Divide operation on unknonw type " + b.getClass)
    }
    case (c: Short) => b match {
      case (d: Double) =>
        c / d
      case (d: Float) =>
        c / d
      case (d: Short) =>
        c / d.toFloat
      case (d: Byte) =>
        c / d.toFloat
      case (d: Int) =>
        c / d.toFloat
      case _ =>
        throw new IllegalArgumentException("Divide operation on unknonw type " + b.getClass)
    }
    case (c: Int) => b match {
      case (d: Double) =>
        c / d
      case (d: Float) =>
        c / d
      case (d: Short) =>
        c / d.toFloat
      case (d: Byte) =>
        c / d.toFloat
      case (d: Int) =>
        c / d.toFloat
      case _ =>
        throw new IllegalArgumentException("Divide operation on unknonw type " + b.getClass)
    }
    case (c: Byte) => b match {
      case (d: Double) =>
        c / d
      case (d: Float) =>
        c / d
      case (d: Short) =>
        c / d.toFloat
      case (d: Byte) =>
        c / d.toFloat
      case (d: Int) =>
        c / d.toFloat
      case _ =>
        throw new IllegalArgumentException("Divide operation on unknonw type " + b.getClass)
    }
    case _ =>
      throw new IllegalArgumentException("Divide operation on unknonw type " + a.getClass)
  }

  def *(a: Any, b: Any): Any = a match {
    case (c: Double) => b match {
      case (d: Double) =>
        c * d
      case (d: Float) =>
        c * d
      case (d: Short) =>
        c * d
      case (d: Byte) =>
        c * d
      case (d: Int) =>
        c * d
      case _ =>
        throw new IllegalArgumentException("Multiple operation on unknonw type " + b.getClass)

    }
    case (c: Float) => b match {
      case (d: Double) =>
        c * d
      case (d: Float) =>
        c * d
      case (d: Short) =>
        c * d
      case (d: Byte) =>
        c * d
      case (d: Int) =>
        c * d
      case _ =>
        throw new IllegalArgumentException("Multiple operation on unknonw type " + b.getClass)
    }
    case (c: Short) => b match {
      case (d: Double) =>
        c * d
      case (d: Float) =>
        c * d
      case (d: Short) =>
        c * d
      case (d: Byte) =>
        c * d
      case (d: Int) =>
        c * d
      case _ =>
        throw new IllegalArgumentException("Multiple operation on unknonw type " + b.getClass)
    }
    case (c: Int) => b match {
      case (d: Double) =>
        c * d
      case (d: Float) =>
        c * d
      case (d: Short) =>
        c * d
      case (d: Byte) =>
        c * d
      case (d: Int) =>
        c * d
      case _ =>
        throw new IllegalArgumentException("Multiple operation on unknonw type " + b.getClass)
    }
    case (c: Byte) => b match {
      case (d: Double) =>
        c * d
      case (d: Float) =>
        c * d
      case (d: Short) =>
        c * d
      case (d: Byte) =>
        c * d
      case (d: Int) =>
        c * d
      case _ =>
        throw new IllegalArgumentException("Multiple operation on unknonw type " + b.getClass)
    }
    case _ =>
      throw new IllegalArgumentException("Multiple operation on unknonw type " + a.getClass)
  }
}

abstract class CompositeColumn[T](val cols: Column*) extends TypedColumn[T] {
  lazy val requiredColumns = cols.map(_.requiredColumns).reduce(_ ++ _)
}

trait OrderSpec extends Column {
  var ascending = true
  def desc = {
    //TODO: we will live with possibility of multiple desc ('firstName desc desc) but lets ignore it now
    ascending = false
    this
  }
}

/*******************/
/* Conditions      */
/*******************/
/* a generic condition */
abstract class Condition(cols: Column*) extends CompositeColumn[Boolean](cols: _*) {
  def and(other: Condition): Condition = new AndCondition(this, other)
  def or(other: Condition): Condition = new OrCondition(this, other)
  override def castToNumeric: NumericColumn = throw new IllegalArgumentException(s"Condition ${this} cannot be numeric")
}

abstract class BinaryCondition(val a: Condition, val b: Condition) extends Condition(a, b) {
  def evaluate(a: Boolean, b: Boolean): Boolean
}

/** a composite condition **/
class AndCondition(a: Condition, b: Condition) extends BinaryCondition(a, b) {
  def name = Symbol(s"(${a} AND ${b})")
  def evaluate(a: Boolean, b: Boolean) = a && b
}

class OrCondition(a: Condition, b: Condition) extends BinaryCondition(a, b) {
  def name = Symbol(s"(${a} OR ${b})")
  def evaluate(a: Boolean, b: Boolean) = a || b
}

class NotCondition(val a: TypedColumn[Boolean]) extends Condition(a) {
  def name = Symbol(s"NOT(${a})")
  def evaluate(a: Boolean) = !a
}

abstract class EqualityCondition(val a: Column, val b: Column) extends Condition(a, b) {
  def evaluate(a: Any, b: Any): Boolean
}

class Equals(a: Column, b: Column) extends EqualityCondition(a, b) {
  def name = Symbol(s"${a} == ${b}")
  def evaluate(a: Any, b: Any) = a == b
}

class NotEquals(a: Column, b: Column) extends EqualityCondition(a, b) {
  def name = Symbol(s"${a} != ${b}")
  def evaluate(a: Any, b: Any) = a != b
}

class LessThan(a: NumericColumn, b: NumericColumn) extends EqualityCondition(a, b) {
  def name = Symbol(s"${a} < ${b}")
  def evaluate(a: Any, b: Any) = Utils.<(a, b)
}

class LessThanEquals(a: NumericColumn, b: NumericColumn) extends EqualityCondition(a, b) {
  def name = Symbol(s"${a} <= ${b}")
  def evaluate(a: Any, b: Any) = Utils.<=(a, b)
}

class GreaterThan(a: NumericColumn, b: NumericColumn) extends EqualityCondition(a, b) {
  def name = Symbol(s"${a} > ${b}")
  def evaluate(a: Any, b: Any) = Utils.>(a, b)
}

class GreaterThanEquals(a: NumericColumn, b: NumericColumn) extends EqualityCondition(a, b) {
  def name = Symbol(s"${a} >= ${b}")
  def evaluate(a: Any, b: Any) = Utils.>=(a, b)
}

abstract class BinaryFunction[T](val a: Column, val b: Column) extends Function[T](a, b) {
  def evaluate(a: Any, b: Any): T
}

class Plus(a: Column, b: Column) extends BinaryFunction[Any](a, b) {
  def name = Symbol(s"(${a} + ${b})")
  def evaluate(a: Any, b: Any) = NumericColumn.+(a, b)
}

class Minus(a: NumericColumn, b: NumericColumn) extends BinaryFunction[Any](a, b) with NumericColumn {
  def name = Symbol(s"(${a} - ${b})")
  def evaluate(a: Any, b: Any) = NumericColumn.-(a, b)
}

class Multiply(a: NumericColumn, b: NumericColumn) extends BinaryFunction[Any](a, b) with NumericColumn {
  def name = Symbol(s"(${a} * ${b})")
  def evaluate(a: Any, b: Any) = NumericColumn.*(a, b)
}

class Divide(a: NumericColumn, b: NumericColumn) extends BinaryFunction[Any](a, b) with NumericColumn {
  def name = Symbol(s"(${a} / ${b})")
  def evaluate(a: Any, b: Any) = NumericColumn./(a, b)
}

/********************/
/* Real data columns*/
/********************/
/* named column */
abstract class DataColumn[T](val name: Symbol) extends TypedColumn[T] with OrderSpec {
  def requiredColumns = Set(name)
}

class NumericDataColumn[T](n: Symbol) extends DataColumn[T](n) with NumericColumn {
  override def castToNumeric = { this }
}

class IntColumn(n: Symbol) extends NumericDataColumn[Int](n)

class LongColumn(n: Symbol) extends NumericDataColumn[Long](n)

class FloatColumn(n: Symbol) extends NumericDataColumn[Float](n)

class DoubleColumn(n: Symbol) extends NumericDataColumn[Double](n)

class StringColumn(n: Symbol) extends DataColumn[String](n)

class BooleanColumn(n: Symbol) extends DataColumn[Boolean](n)

class ByteColumn(n: Symbol) extends DataColumn[Byte](n)

class UntypedColumn(n: Symbol) extends DataColumn[Any](n) {
  override def castToNumeric: NumericColumn = new NumericDataColumn(n)
}

/*******************/
/* Literal columns */
/*******************/
abstract case class LiteralColumn[T](val value: T) extends TypedColumn[T] {
  def name = Symbol(s"${value}")
  def requiredColumns = Set()
}

abstract class NumericLiteral[T](value: T) extends LiteralColumn[T](value) with NumericColumn {
  override def castToNumeric = { this }
}

class IntLiteral(value: Int) extends NumericLiteral[Int](value)

class LongLiteral(value: Long) extends NumericLiteral[Long](value)

class FloatLiteral(value: Float) extends NumericLiteral[Float](value)

class DoubleLiteral(value: Double) extends NumericLiteral[Double](value)

class StringLiteral(value: String) extends LiteralColumn[String](value) {
  override def name = Symbol(s"'${value}'")
}

class BooleanLiteral(value: Boolean) extends LiteralColumn[Boolean](value)

abstract class MultiColumn extends Column {
  def toColumns(schema: Schema): Seq[Column]
}

class AllColumn extends MultiColumn {
  def requiredColumns = Set()

  def name = Symbol("*")

  def toColumns(schema: Schema) = {
    //TODO: make compilation to use MultiColumn interface
    schema.allColumnNames.map(new UntypedColumn(_)).toSeq
  }
}

/********************/
/* Function columns */
/********************/

abstract class Function[T](cols: Column*) extends CompositeColumn[T](cols: _*)

abstract class AggregateFunction[T](cols: Column*) extends Function[T](cols: _*) {
  def createAggregatable(v1: Seq[Any]): Aggregatable[T]
}

abstract class Aggregatable[T <: Any] {
  def aggregate(agg: Aggregatable[T]): Aggregatable[T]

  def value: T
}

case class Summable(val value: Number) extends Aggregatable[Number] {
  override def aggregate(agg: Aggregatable[Number]) = new Summable(NumericColumn.+(value, agg.value).asInstanceOf[Number])

  override def toString = value.toString
}

class Sum(val col: NumericColumn) extends AggregateFunction[Number](col) {
  def name = Symbol(s"SUM(${col})")
  def createAggregatable(v1: Seq[Any]) = new Summable(v1(0).asInstanceOf[Int])
}

case class Countable(val value: Int) extends Aggregatable[Int] {
  override def aggregate(agg: Aggregatable[Int]) = new Countable(value + agg.value.asInstanceOf[Int])
  override def toString = value.toString
}

class Count(val col: Column) extends AggregateFunction[Int](col) {
  def name = Symbol(s"COUNT(${col})")
  def createAggregatable(v1: Seq[Any]) = new Countable(1)
}

case class DistinctCountable(val rows: Set[Row]) extends Aggregatable[Int] {
  def value = rows.size
  override def aggregate(agg: Aggregatable[Int]) = new DistinctCountable(rows ++ agg.asInstanceOf[DistinctCountable].rows)
  override def toString = value.toString
}

class CountDistinct(cols: Column*) extends AggregateFunction[Int](cols: _*) {
  def name = Symbol(s"COUNT(DISTINCT ${cols.mkString(",")})")
  def createAggregatable(v1: Seq[Any]) = new DistinctCountable(Set(new Row(v1.toArray)))
}

class SubSelect(val statement: StatementWrapper) extends Column {
  override def name: Symbol = Symbol("(" + statement.statement().toSql() + ")")
  override def requiredColumns: Set[Symbol] = ???
}

//class Distinct(cols: Seq[Column]) extends CompositeColumn[Any](cols: _*) {
//  def name = Symbol("DISTINCT " + cols.mkString(","))
//}