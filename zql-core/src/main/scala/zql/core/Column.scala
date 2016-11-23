package zql.core

import zql.core.util.Utils

import scala.reflect.ClassTag
import scala.Boolean

/** allow accessing a column from type T **/
abstract class ColumnAccessor[ROW, +T]() extends((ROW) => T){
  def getType: Class[_] = classOf[Any]
}

/** a generic column **/
abstract class Column {

  var alias: Symbol = null

  def name: Symbol

  def as(name: Symbol) = {
    alias = name
    this
  }

  def castToNumeric: NumericColumn = ???

  def getName = if (alias==null) name else alias

  def ===(other: Column): Condition = new Equals(this, other)

  def !==(other: Column): Condition = new NotEquals(this, other)

  def <(other: Column): Condition = new LessThan(castToNumeric, other.castToNumeric)

  def <=(other: Column): Condition = new LessThanEquals(castToNumeric, other.castToNumeric)

  def >(other: Column): Condition = new GreaterThan(castToNumeric, other.castToNumeric)

  def >=(other: Column): Condition = new GreaterThanEquals(castToNumeric, other.castToNumeric)

  def +(other: Column): Function[Any] = new Plus(castToNumeric, other.castToNumeric)

  def -(other: Column): NumericColumn = new Minus(castToNumeric, other.castToNumeric)

  def *(other: Column): NumericColumn = new Multiply(castToNumeric, other.castToNumeric)

  def /(other: Column): NumericColumn = new Divide(castToNumeric, other.castToNumeric)

  def requiredColumns: Set[Symbol]
}

/** column with known type **/
abstract class TypedColumn[T: ClassTag] extends Column {
  val getType = scala.reflect.classTag[T].runtimeClass
}

/** just a tagging interface for numeric column **/
trait NumericColumn extends Column

//TODO: Improve the implementations
object NumericColumn {

  def +(a: Any, b: Any): Any = {
    a.asInstanceOf[Int] + b.asInstanceOf[Int]
  }


  def -(a: Any, b: Any): Any = {
    a.asInstanceOf[Int] - b.asInstanceOf[Int]
  }


  def /(a: Any, b: Any): Any = {
    a.asInstanceOf[Int] / b.asInstanceOf[Int]
  }


  def *(a: Any, b: Any): Any = {
    a.asInstanceOf[Int] * b.asInstanceOf[Int]
  }
}

trait WithAccessor[T] extends Column {
  def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]): ColumnAccessor[ROW, T]
}

abstract class CompositeColumn[T: ClassTag](val cols: Column*) extends TypedColumn[T] with WithAccessor[T] {
  lazy val requiredColumns = cols.map(_.requiredColumns).reduce(_ ++ _)
}

trait OrderSpec extends Column{
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
}

/** a composite condition **/
class AndCondition(val a: TypedColumn[Boolean], val b: TypedColumn[Boolean]) extends Condition(a, b) {
  def name = Symbol(s"AND(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema).asInstanceOf[ColumnAccessor[ROW, Boolean]]
    val bb = compiler.compileColumn[ROW](b, schema).asInstanceOf[ColumnAccessor[ROW, Boolean]]
    def apply(obj: ROW) = aa.apply(obj) && bb.apply(obj)
  }
}

class OrCondition(val a: TypedColumn[Boolean], val b: TypedColumn[Boolean]) extends Condition(a, b) {
  def name = Symbol(s"OR(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema).asInstanceOf[ColumnAccessor[ROW, Boolean]]
    val bb = compiler.compileColumn[ROW](b, schema).asInstanceOf[ColumnAccessor[ROW, Boolean]]
    def apply(obj: ROW) = aa.apply(obj) || bb.apply(obj)
  }
}

class NotCondition(val a: TypedColumn[Boolean]) extends Condition(a){
  def name = Symbol(s"Not(${a.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema).asInstanceOf[ColumnAccessor[ROW, Boolean]]
    def apply(obj: ROW) = !aa.apply(obj)
  }
}

class Equals(val a: Column, val b: Column) extends Condition(a, b){
  def name = Symbol(s"Eq(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = aa.apply(obj).equals(bb.apply(obj))
  }
}

class NotEquals(val a: Column, val b: Column) extends Condition(a, b){
  def name = Symbol(s"NotEq(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = aa.apply(obj) != bb.apply(obj)
  }
}

class LessThan(val a: NumericColumn, val b: NumericColumn) extends Condition(a, b){
  def name = Symbol(s"LessThan(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = Utils.<(aa.apply(obj), (bb.apply(obj)))
  }
}

class LessThanEquals(val a: NumericColumn, val b: NumericColumn) extends Condition(a, b){
  def name = Symbol(s"LessThanEquals(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = Utils.<=(aa.apply(obj), bb.apply(obj))
  }
}


class GreaterThan(val a: NumericColumn, val b: NumericColumn) extends Condition(a, b){
  def name = Symbol(s"GreaterThan(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = Utils.>(aa.apply(obj), bb.apply(obj))
  }
}

class GreaterThanEquals(val a: NumericColumn, val b: NumericColumn) extends Condition(a, b){
  def name = Symbol(s"GreaterThanEquals(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Boolean](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = Utils.>=(aa.apply(obj), bb.apply(obj))
  }
}

class Plus(val a: Column, val b: Column) extends Function[Any](a, b) {
  def name = Symbol(s"Plus(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Any](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = NumericColumn.+(aa.apply(obj), bb.apply(obj))
  }
}

class Minus(val a: NumericColumn, val b: NumericColumn) extends Function[Any](a, b) with NumericColumn {
  def name = Symbol(s"GreaterThanEquals(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Any](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = NumericColumn.-(aa.apply(obj), bb.apply(obj))
  }
}

class Multiply(val a: NumericColumn, val b: NumericColumn) extends Function[Any](a, b) with NumericColumn{
  def name = Symbol(s"GreaterThanEquals(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Any](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = NumericColumn.*(aa.apply(obj), bb.apply(obj))
  }
}

class Divide(val a: NumericColumn, val b: NumericColumn) extends Function[Any](a, b) with NumericColumn{
  def name = Symbol(s"GreaterThanEquals(${a.getName},${b.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Any](){
    val aa = compiler.compileColumn[ROW](a, schema)
    val bb = compiler.compileColumn[ROW](b, schema)
    def apply(obj: ROW) = NumericColumn./(aa.apply(obj), bb.apply(obj))
  }
}
/********************/
/* Real data columns*/
/********************/
/* named column */
abstract class NamedColumn[T: ClassTag](val name: Symbol) extends TypedColumn[T] with OrderSpec {
  def requiredColumns = Set(name)
}


class NumericNamedColumn[T: ClassTag](n: Symbol) extends NamedColumn[T](n) with NumericColumn {
  override def castToNumeric = { this }
}

class IntColumn(n: Symbol) extends NumericNamedColumn[Int](n)

class LongColumn(n: Symbol) extends NumericNamedColumn[Long](n)

class FloatColumn(n: Symbol) extends NumericNamedColumn[Float](n)

class DoubleColumn(n: Symbol) extends NumericNamedColumn[Double](n)

class StringColumn(n: Symbol) extends NamedColumn[String](n)

class BooleanColumn(n: Symbol) extends NamedColumn[Boolean](n)

class UntypedColumn(n: Symbol) extends NamedColumn[Any](n) {
  override def castToNumeric: NumericColumn = new NumericNamedColumn(n)
}

/*******************/
/* Literal columns */
/*******************/
abstract case class LiteralColumn[T: ClassTag](val value: T) extends TypedColumn[T] with WithAccessor[T] {
  def name = Symbol(value.toString)

  def requiredColumns = Set()

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, T](){
    override def apply(v1: ROW): T = value
  }
}

abstract class NumericLiteral[T: ClassTag](value: T) extends LiteralColumn[T](value) with NumericColumn {
  override def castToNumeric = { this }
}

class IntLiteral(value: Int) extends NumericLiteral[Int](value)

class LongLiteral(value: Long) extends NumericLiteral[Long](value)

class FloatLiteral(value: Float) extends NumericLiteral[Float](value)

class DoubleLiteral(value: Double) extends NumericLiteral[Double](value)

class StringLiteralColumn(value: String) extends LiteralColumn[String](value)

class BooleanLiteralColumn(value: Boolean) extends LiteralColumn[Boolean](value)

abstract class MultiColumn extends Column {
  def toColumns(schema: Schema[_]): Seq[Column]
}

class AllColumn extends MultiColumn {
  def requiredColumns = Set()
  def name = Symbol("*")

  def toColumns(schema: Schema[_]) = {
    //TODO: make compilation to use MultiColumn interface
    schema.columnAccessors().keys.map(new UntypedColumn(_)).toSeq
  }
}


/********************/
/* Function columns */
/********************/

abstract class Function[T: ClassTag](cols: Column*) extends CompositeColumn[T](cols: _*)

abstract class AggregateFunction[T: ClassTag](cols: Column*) extends Function[T](cols: _*)


abstract class Aggregatable[T <: Any] {
  def aggregate(agg: Aggregatable[T]): Aggregatable[T]
  def value: T
}

abstract class Summable[T](val value: T) extends Aggregatable[T] {
  override def toString = value.toString
}

class IntSummable(value: Int) extends Summable[Int](value) {
  override def aggregate(agg: Aggregatable[Int]) = new IntSummable(value + agg.value)
}

class LongSummable(value: Long) extends Summable[Long](value) {
  override def aggregate(agg: Aggregatable[Long]) = new LongSummable(value + agg.value)
}

class FloatSummable(value: Float) extends Summable[Float](value) {
  override def aggregate(agg: Aggregatable[Float]) = new FloatSummable(value + agg.value)
}

class DoubleSummable(value: Double) extends Summable[Double](value) {
  override def aggregate(agg: Aggregatable[Double]) = new DoubleSummable(value + agg.value)
}

class Sum(val col: NumericColumn) extends AggregateFunction[Summable[_]](col) with WithAccessor[Summable[_]] {
  def name = Symbol(s"SUM(${col.getName})")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]): ColumnAccessor[ROW, Summable[_]] = new ColumnAccessor[ROW, Summable[_]](){
    val colAccessor = compiler.compileColumn[ROW](col, schema)
    override def apply(obj: ROW): Summable[_] = {
      val columnType = colAccessor.getType
      new IntSummable(colAccessor.apply(obj).asInstanceOf[Int] )
//      if (columnType.equals(classOf[Int])){
//        new IntSummable(colAccessor.apply(obj).asInstanceOf[Int] )
//      } else if (columnType.equals(classOf[Long])){
//        new LongSummable(colAccessor.apply(obj).asInstanceOf[Long])
//      } else if (columnType.equals(classOf[Float])){
//        new FloatSummable(colAccessor.apply(obj).asInstanceOf[Float] )
//      }else if (columnType.equals(classOf[Double])){
//        new DoubleSummable(colAccessor.apply(obj).asInstanceOf[Double] )
//      } else {
//        throw new IllegalArgumentException("Unknown column type for sum")
//      }
    }
  }
}

class Countable(val value: Number) extends Aggregatable[Number] {
  override def aggregate(agg: Aggregatable[Number]) = {
    new Countable(value.intValue() + agg.asInstanceOf[Countable].value.intValue)
  }

  override def toString = value.toString
}

class Count(val col: Column) extends AggregateFunction[Countable](col) with WithAccessor[Countable] {
  def name = Symbol(s"COUNT(${col.getName}")

  override def getColumnAccessor[ROW](compiler: Compiler[_], schema: Schema[ROW]) = new ColumnAccessor[ROW, Countable](){
    def apply(obj: ROW) = new Countable(1)
  }
}
