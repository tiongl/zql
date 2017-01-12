package zql.rowbased

import zql.core.{ Executable, JoinedTable, Statement, Table }
import zql.schema.Schema
import zql.sql.SqlGenerator

import scala.collection.mutable
import scala.reflect.ClassTag

abstract class StreamData[R: ClassTag] extends RowBasedData[R] {
  def asQueue[T: ClassTag]: mutable.Queue[T]

  def asRowQueue: mutable.Queue[Row]

  def getSnapshotCollector: SnapshotCollector[R]
}

abstract class StreamingTable[R: ClassTag](schema: Schema, streamData: StreamData[R]) extends RowBasedTable[R](schema) {

  override def data: RowBasedData[R] = streamData

  override def collectAsRowList: List[Row] = streamData.asList

  def collectAsQueue[T: ClassTag](): mutable.Queue[T]

  def collectAsRowQueue(): mutable.Queue[Row]

  def getSnapshotCollector() = streamData.getSnapshotCollector
}

abstract class SnapshotCollector[R: ClassTag] {
  def collect: List[R]
}