package zql

import zql.core.ReflectionColumnDef
import zql.core.util.Utils

import scala.reflect.ClassTag

/**
 * Created by tiong on 7/4/16.
 */
package object core {

  implicit def untypedColumn(symbol: Symbol): UntypedColumn = {
    new UntypedColumn(symbol)
  }

  implicit def stringLiteralColumn(string: String): StringLiteral = new StringLiteral(string)

  implicit def intLiteral(number: Int): IntLiteral = new IntLiteral(number)

  implicit def floatLiteral(number: Float): FloatLiteral = new FloatLiteral(number)

  implicit def longLiteral(number: Long): LongLiteral = new LongLiteral(number)

  implicit def doubleLiteral(number: Double): DoubleLiteral = new DoubleLiteral(number)

  implicit def booleanColumn(bool: Boolean): BooleanLiteral = new BooleanLiteral(bool)

  implicit def toNumeric(col: UntypedColumn): NumericColumn = new NumericDataColumn(col.name)

  implicit def statmentToColumn(stmt: StatementWrapper): Column = new SubSelect(stmt)

  implicit def select(cols: Column*) = new Selected(false, cols: _*)

  implicit def selectDistinct(cols: Column*) = new Selected(true, cols: _*)

  implicit def statementAsTable(stmt: StatementWrapper): Table = stmt.statement().compile.execute()

  //purposely not support distinct this way as implementation is complicated
  //  implicit def distinct(cols: Column*): Distinct = new Distinct(cols)

  def sum(col: UntypedColumn): Sum = sum(new NumericDataColumn(col.name)) //upgrade to numeric column

  def sum(col: NumericColumn): Sum = new Sum(col)

  //use capitalized due to conflict with not
  def NOT(col: UntypedColumn): NotCondition = NOT(new BooleanColumn(col.name))

  def NOT(col: BooleanColumn): NotCondition = new NotCondition(col)

  def NOT(col: Condition): NotCondition = new NotCondition(col)

  def *(): Column = new AllColumn()

  def count(col: Column) = new Count(col)

  def countDistinct(cols: Column*) = new CountDistinct(cols: _*)

  //Column definitions
  implicit def symToReflectionColumnDef[ROW: ClassTag](sym: Symbol): ReflectionColumnDef[ROW] = {
    new ReflectionColumnDef[ROW](sym)
  }

}
