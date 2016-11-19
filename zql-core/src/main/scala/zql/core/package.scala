package zql

/**
  * Created by tiong on 7/4/16.
  */
package object core {

  implicit def genericNamedColumn(symbol: Symbol): GenericNamedColumn = {
    new GenericNamedColumn(symbol)
  }


  implicit def stringLiteralColumn(string: String): StringLiteralColumn = new StringLiteralColumn(string)

  implicit def numericColumn(number: Number): NumericLiteralColumn = new NumericLiteralColumn(number)

  def SUM(col: GenericNamedColumn): Sum = SUM(new NumericNamedColumn(col.name)) //upgrade to numeric column

  def SUM(col: NumericNamedColumn): Sum = new Sum(col)

  def NOT(col: GenericNamedColumn): NotCondition = NOT(new BoolNamedColumn(col.name))

  def NOT(col: BoolNamedColumn): NotCondition = new NotCondition(col)

  def NOT(col: Condition): NotCondition = new NotCondition(col)

  def *(): Column = new AllColumn()

}
