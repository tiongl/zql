package zql.flink

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.Accumulator
import org.apache.flink.api.common.functions.{RichMapFunction, RichFunction, ReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import zql.core.{JoinType, CompileOption}
import zql.rowbased._
import zql.schema.Schema
import zql.util.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class DataStreamData[ROW: ClassTag](val stream: DataStream[ROW], val option: CompileOption = new CompileOption) extends StreamData[ROW] {

  implicit val rowTypeInfo = new BasicRowTypeInfo

  implicit lazy val typeInfo = rowTypeInfo.asInstanceOf[TypeInformation[ROW]]

  implicit def toStreamData[T: ClassTag](str: DataStream[T]): DataStreamData[T] = new DataStreamData[T](str, option)

  override def asQueue[T: ClassTag]: mutable.Queue[T] = ???

  override def getSnapshotCollector: SnapshotCollector[ROW] = {
    new DataStreamSnapshotCollector[ROW](stream)
  }

  override def asRowQueue: mutable.Queue[Row] =  {
    val queue = new mutable.Queue[Row]()
    stream.addSink{
      data => queue.enqueue(data.asInstanceOf[Row])
    }
    queue
  }


  override def isLazy: Boolean = true

  override def reduce(reduceFunc: (ROW, ROW) => ROW): RowBasedData[ROW] = //stream.keyBy(r => r).reduce(reduceFunc)
  {
    val func = DataStreamData.reduceStateFunc[ROW](reduceFunc)
    stream.keyBy(r => Row.EMPTY_ROW).mapWithState[ROW, ROW](func)//.setParallelism(1)
  }

  override def filter(filter: (ROW) => Boolean): RowBasedData[ROW] = stream.filter(filter)

  override def distinct(): RowBasedData[ROW] = {
    val func = DataStreamData.distinctStateFunc[ROW]
    stream.keyBy(r => r).mapWithState[ROW, ROW](func)//.setParallelism(1)
  }

  override def crossJoin[T: ClassTag](other: RowBasedData[T], leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T]): RowBasedData[Row] =
    throw new UnsupportedOperationException("crossjoin is not supported")

  override def withOption(opt: CompileOption): RowBasedData[ROW] = new DataStreamData[ROW](stream, opt)

  override def joinWithKey[T: ClassTag](other: RowBasedData[T],
                                             leftKeyFunc: RowBuilder[ROW], rightKeyFunc: RowBuilder[T],
                                             leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T],
                                             joinType: JoinType): RowBasedData[Row] = {
    other match {
      case dsd: DataStreamData[T] =>
        val joinedStream = stream.join.coGroup(dsd.stream)
        joinType match {
          case JoinType.innerJoin =>
            val joinFunc = DataStreamData.joinFunc(leftSelect, rightSelect)
            joinedStream.where(leftKeyFunc(_)).equalTo(rightKeyFunc(_)).window(GlobalWindows.create()).apply(joinFunc)
        }

//        val joinedStream = stream.coGroup(dsd.stream)
//        joinType match {
//          case JoinType.innerJoin =>
//            val joinFunc = DataStreamData.joinFunc(leftSelect, rightSelect)
//            joinedStream.where(leftKeyFunc(_)).equalTo(rightKeyFunc(_)).window(GlobalWindows.create()).apply(joinFunc)
//        }
      }
  }

  override def groupBy(keyFunc: (ROW) => Row, selectFunc: (ROW) => Row, groupFunc: (Row, Row) => Row): RowBasedData[Row] = {
    val func = DataStreamData.mapReduceStateFunc[ROW](selectFunc, groupFunc)
    stream.keyBy(keyFunc(_)).mapWithState(func)
  }

  override def asList[T]: List[T] = ???

  override def map[T: ClassTag](mapFunc: (ROW) => T): RowBasedData[T] = {
    stream.map(mapFunc)(rowTypeInfo.asInstanceOf[TypeInformation[T]])
  }
}

object DataStreamData {
  def joinFunc[L, R](leftSelect: RowBuilder[L], rightSelect: RowBuilder[R]) = (l: Iterator[L], r: Iterator[R], collector: Collector[Row]) =>
    {
      val rows = Utils.crossProductWithFunc[L, R](l, r, leftSelect, rightSelect)
      rows.foreach(collector.collect(_))
    }

  def distinctStateFunc[T] = (t: T, state: Option[T]) => {
    state match {
      case None => (t, Option(t))
      case Some(s) => {
        (s, Option(s))
      }
    }
  }

  def addToSetStateFunc[T](func: (T) => Row) = (t: T, state: Option[Seq[Row]]) => {
    state match {
      case None =>
        val set = Seq(func(t))
        (set, Option(set))
      case Some(current) => {
        val set = Seq(func(t)) ++ current
        (set, Option(set))
      }
    }
  }

  def reduceStateFunc[T](reduceFunc: (T, T) => T) = (t: T, state: Option[T]) => {
    state match {
      case None => (t, Option(t))
      case Some(s) => {
        val newValue = reduceFunc.apply(t, s)
        (newValue, Option(newValue))
      }
    }
  }

  def mapReduceStateFunc[ROW](mapFunc: (ROW) => Row, reduceFunc: (Row, Row) => Row) = (t: ROW, state: Option[Row]) => {
    val row = mapFunc(t)
    state match {
      case None => (row, Option(row))
      case Some(s) => {
        val newValue = reduceFunc.apply(row, s)
        (newValue, Option(newValue))
      }
    }
  }

}

class ResultAccumulator[T] extends Accumulator[T, List[T]]{
  val result = new mutable.ArrayBuffer[T]
  override def getLocalValue: List[T] = result.toList

  override def resetLocal(): Unit = result.clear()

  override def merge(other: Accumulator[T, List[T]]): Unit = result.appendAll(other.getLocalValue)

  override def add(value: T): Unit = result.append(value)
}

class DataStreamSnapshotCollector[T: ClassTag](stream: DataStream[T])(implicit val info: TypeInformation[T]) extends SnapshotCollector[T]{
  val resultSink = new ResultSink[T](stream.toString)
  stream.addSink(resultSink)

  override def collect: List[T] = throw new IllegalArgumentException("Use collect(jobresult) instead")

  def collect(jobExecutionResult: JobExecutionResult): List[T] = {
    val result = jobExecutionResult.getAccumulatorResult[List[T]](stream.toString)
    result
  }
}

class ResultSink[T](resultName: String) extends RichSinkFunction[T]{
  val results = new ResultAccumulator[T]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    getRuntimeContext.addAccumulator(resultName, results)
  }

  override def invoke(value: T): Unit = {
    results.add(value)
  }
}

class FlinkStreamingTable[T: ClassTag](schema: Schema, stream: DataStreamData[T]) extends StreamingTable[T](schema, stream){
  override def collectAsQueue[T: ClassTag](): mutable.Queue[T] = ???

  override def collectAsRowQueue(): mutable.Queue[Row] = ???

  override def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T] = new FlinkStreamingTable(newSchema, rowBased.asInstanceOf[DataStreamData[T]])
}
