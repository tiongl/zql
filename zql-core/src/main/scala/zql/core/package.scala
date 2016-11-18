package zql

/**
  * Created by tiong on 7/4/16.
  */
package object core {

  implicit def numbericNamedColumn(col: GenericNamedColumn): NumericNamedColumn = {
    new NumericNamedColumn(col.name)
  }

//  implicit def numbericNamedColumn(symbol: Symbol): NumericNamedColumn = {
//    new NumericNamedColumn(symbol)
//  }
//
//  implicit def stringNamedColumn(symbol: Symbol): StringNamedColumn = {
//    new StringNamedColumn(symbol)
//  }

  implicit def genericNamedColumn(symbol: Symbol): GenericNamedColumn = {
    new GenericNamedColumn(symbol)
  }


  implicit def stringLiteralColumn(string: String): StringLiteralColumn = new StringLiteralColumn(string)

  implicit def numericColumn(number: Number): NumericLiteralColumn = new NumericLiteralColumn(number)

  def SUM(col: GenericNamedColumn): Sum = SUM(new NumericNamedColumn(col.name)) //upgrade to numeric column

  def SUM(col: NumericNamedColumn): Sum = new Sum(col)

}
