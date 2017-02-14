package zql.rowbased

import zql.core.{ JoinType, CompileOption }
import zql.rowbased.RowBuilder.BUILDER

import scala.reflect.ClassTag

abstract class RowBuilder[T](val card: Int) extends BUILDER[T] with Serializable

object RowBuilder {
  type BUILDER[T] = (T) => Row
}

abstract class RowBasedData[ROW: ClassTag] {
  def option: CompileOption
  def withOption(option: CompileOption): RowBasedData[ROW]
  def slice(offset: Int, until: Int): RowBasedData[ROW]
  def filter(filter: (ROW) => Boolean): RowBasedData[ROW]
  def groupBy(keyFunc: (ROW) => Row, selectFunc: (ROW) => Row,
    groupFunc: (Row, Row) => Row): RowBasedData[Row]

  def reduce(reduceFunc: (ROW, ROW) => ROW): RowBasedData[ROW]
  def map[T: ClassTag](mapFunc: (ROW) => T): RowBasedData[T]
  def sortBy[T: ClassTag](keyFunc: (ROW) => T, ordering: Ordering[T]): RowBasedData[ROW]
  def size: Int
  def asList[T]: List[T]
  def isLazy: Boolean
  def distinct(): RowBasedData[ROW]
  def crossJoin[T: ClassTag](other: RowBasedData[T], leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T]): RowBasedData[Row]
  def joinWithKey[T: ClassTag](
    other: RowBasedData[T],
    leftKeyFunc: RowBuilder[ROW], rightKeyFunc: RowBuilder[T],
    leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T],
    joinType: JoinType
  ): RowBasedData[Row]

  /** for doing any last step of result preparation **/
  def resultData: RowBasedData[ROW] = this
}

