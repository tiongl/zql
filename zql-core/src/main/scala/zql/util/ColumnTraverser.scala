package zql.util

import zql.core._

/**
 * This is similar to column visitor exception use pattern matching to make it simpler to understand
 *
 * @tparam R
 */
abstract class ColumnTraverser[R, C] {

  def traverse(col: Column, context: C): R = col match {
    //some base cases
    case dc: DataColumn => traverseDataColumn(dc, context)
    case os: OrderSpec => traverseOrderSpec(os, context)
    case cond: Condition => traverseCondition(cond, context)
    case lc: LiteralColumn[_] => traverseLiteral(lc, context)
    case func: Function[_] => traverseFunction(func, context)
    case ac: AllColumn => handleAllColumn(ac, context)
    case ss: SubSelect => handleSubSelect(ss, context)
  }

  def traverseCondition(cond: Condition, context: C): R = cond match {
    case bc: BinaryCondition => traverseBooleanCondition(bc, context)
    case nc: NotCondition => handleNotCondition(nc, context)
    case ec: EqualityCondition => traverseEqualityCondition(ec, context)
  }

  def traverseBooleanCondition(bc: BinaryCondition, context: C): R = bc match {
    case ac: AndCondition => handleAndCondition(ac, context)
    case bc: OrCondition => handleOrCondition(bc, context)
  }

  def traverseEqualityCondition(ec: EqualityCondition, context: C): R = ec match {
    case eq: Equals => handleEquals(ec, context)
    case ne: NotEquals => handleNotEquals(ne, context)
    case lt: LessThan => handleLessThan(lt, context)
    case lte: LessThanEquals => handleLessThanEquals(lte, context)
    case gt: GreaterThan => handleGreaterThan(gt, context)
    case gte: GreaterThanEquals => handleGreaterThanEquals(gte, context)
  }

  def traverseFunction(func: Function[_], context: C): R = func match {
    case bf: BinaryFunction[_] => handleBinaryFunction(bf, context)
    case af: AggregateFunction[_] => handleAggregateFunction(af, context)
  }

  def traverseDataColumn(data: DataColumn, context: C): R = ???

  def traverseOrderSpec(os: OrderSpec, context: C): R = ???

  def traverseLiteral(literal: LiteralColumn[_], context: C): R = ???

  def handleNotCondition(nc: NotCondition, context: C): R = ???

  def handleAllColumn(ac: AllColumn, context: C): R = ???

  def handleBinaryFunction(func: BinaryFunction[_], context: C): R = ???

  def handleEquals(ec: EqualityCondition, context: C): R = ???

  def handleNotEquals(ne: NotEquals, context: C): R = ???

  def handleLessThan(lt: LessThan, context: C): R = ???

  def handleLessThanEquals(lte: LessThanEquals, context: C): R = ???

  def handleGreaterThan(gt: GreaterThan, context: C): R = ???

  def handleGreaterThanEquals(egte: Any, context: C): R = ???

  def handleAndCondition(ac: AndCondition, context: C): R = ???

  def handleOrCondition(bc: OrCondition, context: C): R = ???

  def handleAggregateFunction(af: AggregateFunction[_], context: C): R = ???

  def handleSubSelect(ss: SubSelect, context: C): R = ???
}
