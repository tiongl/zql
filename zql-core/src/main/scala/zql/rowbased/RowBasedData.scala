package zql.rowbased

import zql.core.CompileOption

import scala.reflect.ClassTag

abstract class RowBasedData[ROW: ClassTag] {
  def option: CompileOption
  def withOption(option: CompileOption): RowBasedData[ROW]
  def slice(offset: Int, until: Int): RowBasedData[ROW]
  def select(r: (ROW) => Row): RowBasedData[Row]
  def filter(filter: (ROW) => Boolean): RowBasedData[ROW]
  def groupBy(keyFunc: (ROW) => Row, selectFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row]
  def reduce(reduceFunc: (ROW, ROW) => ROW): RowBasedData[ROW]
  def map(mapFunc: (ROW) => Row): RowBasedData[Row]
  def sortBy(keyFunc: (ROW) => Row, ordering: Ordering[Row], tag: ClassTag[Row]): RowBasedData[ROW]
  def size: Int
  def asList: List[ROW]
  def isLazy: Boolean
  def distinct(): RowBasedData[ROW]
  def join(other: RowBasedData[Row], jointPoint: (Row) => Boolean): RowBasedData[Row]
}
