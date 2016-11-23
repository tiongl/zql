package zql

import zql.core.util.Utils

/**
  * Created by tiong on 7/4/16.
  */
package object core {

  implicit def untypedColumn(symbol: Symbol): UntypedColumn = {
    new UntypedColumn(symbol)
  }

  implicit def stringLiteralColumn(string: String): StringLiteralColumn = new StringLiteralColumn(string)

  implicit def intLiteral(number: Int): IntLiteral = new IntLiteral(number)

  implicit def floatLiteral(number: Float): FloatLiteral = new FloatLiteral(number)

  implicit def longLiteral(number: Long): LongLiteral = new LongLiteral(number)

  implicit def doubleLiteral(number: Double): DoubleLiteral = new DoubleLiteral(number)

  implicit def booleanColumn(bool: Boolean): BooleanLiteralColumn = new BooleanLiteralColumn(bool)

  implicit def toNumeric(col: UntypedColumn): NumericColumn = new NumericNamedColumn(col.name)

  def sum(col: UntypedColumn): Sum = sum(new NumericNamedColumn(col.name)) //upgrade to numeric column

  def sum(col: NumericColumn): Sum = new Sum(col)

  //use capitalized due to conflict with not
  def NOT(col: UntypedColumn): NotCondition = NOT(new BooleanColumn(col.name))

  def NOT(col: BooleanColumn): NotCondition = new NotCondition(col)

  def NOT(col: Condition): NotCondition = new NotCondition(col)


  def *(): Column = new AllColumn()

  def count(col: NamedColumn[_]) = new Count(col)



}
