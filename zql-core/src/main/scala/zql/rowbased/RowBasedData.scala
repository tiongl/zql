package zql.rowbased

import zql.core.{ JoinType, CompileOption }

import scala.reflect.ClassTag

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
  def joinData[T: ClassTag](other: RowBasedData[T], jointPoint: (Row) => Boolean,
    rowifier: (ROW, T) => Row): RowBasedData[Row]
  def joinData[T: ClassTag](
    other: RowBasedData[T],
    leftKeyFunc: (ROW) => Row, rightKeyFunc: (T) => Row,
    leftSelect: (ROW) => Row, rightSelect: (T) => Row,
    joinType: JoinType
  ): RowBasedData[Row]

  /** for doing any last step of result preparation **/
  def resultData: RowBasedData[ROW] = this
}

class RowCombiner[A, B] extends ((A, B) => Row) with Serializable {
  override def apply(v1: A, v2: B): Row = {
    val r1 = v1.asInstanceOf[Row]
    val r2 = v2.asInstanceOf[Row]
    new Row(r1.data ++ r2.data)
  }
}
