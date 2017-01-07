package zql.core

import zql.core.util.Utils
import zql.sql.{ SqlGenerator, DefaultSqlGenerator }

import scala.reflect.ClassTag

/** a generic column **/
abstract class Column extends Serializable {

  var alias: Symbol = null

  def as(name: Symbol) = {
    alias = name
    this
  }

  def dataType: Class[_]

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

  def requiredColumns: Seq[Symbol]

  def name = Symbol(toSql(new DefaultSqlGenerator()))

  def toSql(gen: SqlGenerator): String
}

/** column with a type **/
abstract class TypedColumn[T: ClassTag] extends Column {
  val dataType = scala.reflect.classTag[T].runtimeClass
}

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

abstract class CompositeColumn[T: ClassTag](val cols: Column*) extends TypedColumn[T] {
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

abstract class BinaryCondition(val a: Condition, val b: Condition, val operator: String) extends Condition(a, b) {
  def evaluate(a: Boolean, b: Boolean): Boolean
  def toSql(gen: SqlGenerator) = {
    val aSql = gen.visit(a, "")
    val bSql = gen.visit(b, "")
    s"(${aSql} ${operator} ${bSql})"
  }
}

/** a composite condition **/
class AndCondition(a: Condition, b: Condition) extends BinaryCondition(a, b, "AND") {
  def evaluate(a: Boolean, b: Boolean) = a && b
}

class OrCondition(a: Condition, b: Condition) extends BinaryCondition(a, b, "OR") {
  def evaluate(a: Boolean, b: Boolean) = a || b
}

class NotCondition(val a: TypedColumn[Boolean]) extends Condition(a) {
  def evaluate(a: Boolean) = !a
  def toSql(gen: SqlGenerator) = {
    val aStr = gen.visit(a, null)
    s"NOT(${aStr})"
  }
}

abstract class EqualityCondition(val a: Column, val b: Column, val operator: String) extends Condition(a, b) {
  def evaluate(a: Any, b: Any): Boolean
  def toSql(gen: SqlGenerator) = {
    val aSql = gen.visit(a, null)
    val bSql = gen.visit(b, null)
    s"${aSql} ${operator} ${bSql}"
  }
}

class Equals(a: Column, b: Column) extends EqualityCondition(a, b, "==") {
  def evaluate(a: Any, b: Any) = a == b

}

class NotEquals(a: Column, b: Column) extends EqualityCondition(a, b, "!=") {
  def evaluate(a: Any, b: Any) = a != b
}

class LessThan(a: NumericColumn, b: NumericColumn) extends EqualityCondition(a, b, "<") {
  def evaluate(a: Any, b: Any) = Utils.<(a, b)
}

class LessThanEquals(a: NumericColumn, b: NumericColumn) extends EqualityCondition(a, b, "<=") {
  def evaluate(a: Any, b: Any) = Utils.<=(a, b)
}

class GreaterThan(a: NumericColumn, b: NumericColumn) extends EqualityCondition(a, b, ">") {
  def evaluate(a: Any, b: Any) = Utils.>(a, b)
}

class GreaterThanEquals(a: NumericColumn, b: NumericColumn) extends EqualityCondition(a, b, ">=") {
  def evaluate(a: Any, b: Any) = Utils.>=(a, b)
}

abstract class BinaryFunction[T: ClassTag](val funcName: String, val a: Column, val b: Column) extends Function[T](funcName, a, b) {
  def evaluate(a: Any, b: Any): T
  override def toSql(gen: SqlGenerator) = {
    val aSql = gen.visit(a, null)
    val bSql = gen.visit(b, null)
    s"(${aSql} ${funcName} ${bSql})"
  }
}

class Plus(a: Column, b: Column) extends BinaryFunction[Any]("+", a, b) {
  def evaluate(a: Any, b: Any) = NumericColumn.+(a, b)
}

class Minus(a: NumericColumn, b: NumericColumn) extends BinaryFunction[Any]("-", a, b) with NumericColumn {
  def evaluate(a: Any, b: Any) = NumericColumn.-(a, b)
}

class Multiply(a: NumericColumn, b: NumericColumn) extends BinaryFunction[Any]("*", a, b) with NumericColumn {
  def evaluate(a: Any, b: Any) = NumericColumn.*(a, b)
}

class Divide(a: NumericColumn, b: NumericColumn) extends BinaryFunction[Any]("/", a, b) with NumericColumn {
  def evaluate(a: Any, b: Any) = NumericColumn./(a, b)
}

/********************/
/* Real data columns*/
/********************/
trait DataColumn extends Column with OrderSpec

abstract class TypedDataColumn[T: ClassTag](override val name: Symbol) extends TypedColumn[T] with DataColumn {
  def requiredColumns = Seq(name)
  def toSql(gen: SqlGenerator) = {
    //TODO: This is temporary, until we have normalization of statement
    name.name.replace("_", ".")
  }
}

class NumericDataColumn[T: ClassTag](name: Symbol) extends TypedDataColumn[T](name) with NumericColumn {
  override def castToNumeric = { this }
}

class IntColumn(n: Symbol) extends NumericDataColumn[Int](n)

class LongColumn(n: Symbol) extends NumericDataColumn[Long](n)

class FloatColumn(n: Symbol) extends NumericDataColumn[Float](n)

class DoubleColumn(n: Symbol) extends NumericDataColumn[Double](n)

class StringColumn(n: Symbol) extends TypedDataColumn[String](n)

class BooleanColumn(n: Symbol) extends TypedDataColumn[Boolean](n)

class ByteColumn(n: Symbol) extends TypedDataColumn[Byte](n)

class UntypedColumn(n: Symbol) extends TypedDataColumn[Any](n) {
  override def castToNumeric: NumericColumn = new NumericDataColumn(n)

}

/*******************/
/* Literal columns */
/*******************/
abstract case class LiteralColumn[T: ClassTag](val value: T) extends TypedColumn[T] {
  def requiredColumns = Seq()
  def toSql(gen: SqlGenerator) = {
    s"${value}"
  }
}

abstract class NumericLiteral[T: ClassTag](value: T) extends LiteralColumn[T](value) with NumericColumn {
  override def castToNumeric = { this }
}

class IntLiteral(value: Int) extends NumericLiteral[Int](value)

class LongLiteral(value: Long) extends NumericLiteral[Long](value)

class FloatLiteral(value: Float) extends NumericLiteral[Float](value)

class DoubleLiteral(value: Double) extends NumericLiteral[Double](value)

class StringLiteral(value: String) extends LiteralColumn[String](value) {
  override def toSql(gen: SqlGenerator) = s"'${value}'"
}

class BooleanLiteral(value: Boolean) extends LiteralColumn[Boolean](value)

trait MultiColumn extends Column {
  def toColumns(schema: Schema): Seq[Column]
}

class AllColumn extends MultiColumn {
  def requiredColumns = Seq()

  def dataType = ???

  def toColumns(schema: Schema) = {
    //TODO: make compilation to use MultiColumn interface
    schema.allColumnNames.map(new UntypedColumn(_)).toSeq
  }

  def toSql(gen: SqlGenerator) = "*"
}

/********************/
/* Function columns */
/********************/

abstract class Function[T: ClassTag](funcName: String, cols: Column*) extends CompositeColumn[T](cols: _*) {
  def toSql(gen: SqlGenerator) = {
    val columns = cols.map(c => gen.visit(c, null)).mkString(", ")
    s"${funcName}(${columns})"
  }
}

abstract class AggregateFunction[T: ClassTag](funcName: String, cols: Column*) extends Function[T](funcName, cols: _*) {
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

class Sum(val col: NumericColumn) extends AggregateFunction[Number]("SUM", col) {
  def createAggregatable(v1: Seq[Any]) = new Summable(v1(0).asInstanceOf[Int])
}

case class Countable(val value: Int) extends Aggregatable[Int] {
  override def aggregate(agg: Aggregatable[Int]) = new Countable(value + agg.value.asInstanceOf[Int])
  override def toString = value.toString
}

class Count(val col: Column) extends AggregateFunction[Int]("COUNT", col) {
  def createAggregatable(v1: Seq[Any]) = new Countable(1)
}

case class DistinctCountable(val rows: Set[Row]) extends Aggregatable[Int] {
  def value = rows.size
  override def aggregate(agg: Aggregatable[Int]) = new DistinctCountable(rows ++ agg.asInstanceOf[DistinctCountable].rows)
  override def toString = value.toString
}

class CountDistinct(cols: Column*) extends AggregateFunction[Int]("COUNT_DISTINCT", cols: _*) {
  val funcName = "COUNT(DISTINCT"
  def createAggregatable(v1: Seq[Any]) = new DistinctCountable(Set(new Row(v1.toArray)))
  override def toSql(gen: SqlGenerator) = {
    val columns = cols.map(c => gen.visit(c, null)).mkString(", ")
    s"COUNT(DISTINCT ${columns})"
  }

}

class SubSelect(val statement: StatementWrapper) extends Column {
  override def requiredColumns: Seq[Symbol] = Seq() //non of other statement business

  override def dataType = classOf[Any]

  def toSql(gen: SqlGenerator) = {
    val stmtSql = gen.generateSql(statement.statement())
    s"(${stmtSql})"
  }

}