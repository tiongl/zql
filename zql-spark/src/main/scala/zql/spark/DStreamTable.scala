package zql.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ State, Time, StateSpec }
import org.apache.spark.streaming.dstream.{ MapWithStateDStream, DStream }
import zql.core._
import zql.rowbased._
import zql.schema.Schema
import zql.util.Utils

import scala.collection.mutable
import scala.reflect.ClassTag

class DStreamData[ROW: ClassTag](val stream: DStream[ROW], val option: CompileOption = new CompileOption, val stated: Boolean = false) extends StreamData[ROW] {

  implicit def streamToStreamData[T: ClassTag](stream: DStream[T]): DStreamData[T] = stream match {
    case statedstream: MapWithStateDStream[_, _, _, _] =>
      new DStreamData(statedstream.stateSnapshots().map[T](_._2.asInstanceOf[T]).asInstanceOf[DStream[T]], option, true)
    case _ => new DStreamData(stream, option, stated)
  }

  override def isLazy: Boolean = true

  override def reduce(reduceFunc: (ROW, ROW) => ROW): RowBasedData[ROW] = {

    val reduceRow = reduceFunc.asInstanceOf[(Row, Row) => Row]
    val groupByFunc = StateSpec.function(DStreamData.reduceFunc(reduceRow) _)
    val rowStream = stream.asInstanceOf[DStream[Row]]
    val stateStream = rowStream.map(r => (Row.EMPTY_ROW.asInstanceOf[Row], r)).mapWithState(groupByFunc)
    stateStream.asInstanceOf[DStream[ROW]]
  }

  override def filter(filter: (ROW) => Boolean): RowBasedData[ROW] = stream.filter(filter)

  override def distinct(): RowBasedData[ROW] = {
    val distinctFunc = StateSpec.function(DStreamData.distinctFunc _)
    val rowStream = stream.asInstanceOf[DStream[Row]]
    val stateStream = rowStream.map(r => (r, r)).mapWithState(distinctFunc)
    stateStream.asInstanceOf[DStream[ROW]]
  }

  override def withOption(option: CompileOption): RowBasedData[ROW] = new DStreamData(stream, option, stated)

  override def groupBy(keyFunc: (ROW) => Row, selectFunc: (ROW) => Row, aggFunc: (Row, Row) => Row): RowBasedData[Row] = {
    val groupByFunc = StateSpec.function(DStreamData.reduceFunc(aggFunc) _)
    stream.map(t => (keyFunc(t), selectFunc(t))).mapWithState(groupByFunc)
  }

  override def asList[T]: List[T] = ???
  //  {
  //    stream.foreachRDD {
  //      rdd =>
  //        println(rdd.collect().mkString(","))
  //    }
  //    List()
  //  }

  override def asQueue[T: ClassTag]: mutable.Queue[T] = {
    val queue = mutable.Queue[T]()
    stream.foreachRDD {
      rdd =>
        queue.enqueue(rdd.collect().toSeq.asInstanceOf[Seq[T]]: _*)
    }
    queue
  }

  override def getSnapshotCollector: SnapshotCollector[ROW] = {
    stated match {
      case false =>
        new AccumulativeCollector[ROW](stream)
      case true =>
        new SnapshotStreamCollector[ROW](stream)
    }
    //    new DStreamSnapshotCollector[ROW](stream)
  }

  override def asRowQueue = {
    val queue = mutable.Queue[Row]()
    stream.foreachRDD {
      rdd =>
        queue.enqueue(rdd.collect().toSeq.asInstanceOf[Seq[Row]]: _*)
    }
    queue
  }

  override def map[T: ClassTag](mapFunc: (ROW) => T): RowBasedData[T] = stream.map(mapFunc(_))

  override def crossJoin[T: ClassTag](other: RowBasedData[T], leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T]): RowBasedData[Row] =
    throw new IllegalArgumentException("crossjoin is not supported for streaming")

  override def joinWithKey[T: ClassTag](
    other: RowBasedData[T],
    leftKeyFunc: RowBuilder[ROW], rightKeyFunc: RowBuilder[T],
    leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T],
    joinType: JoinType
  ): RowBasedData[Row] = {
    import DStreamData._
    other match {
      case rightTable: DStreamData[T] =>
        val groupByFunc = StateSpec.function(DStreamData.groupFunc _)
        val leftStream = stream.map[(Row, Row)] {
          r: ROW => (leftKeyFunc(r), leftSelect(r))
        }.mapWithState(groupByFunc)
        val rightStream = rightTable.stream.map[(Row, Row)] {
          r: T => (rightKeyFunc(r), rightSelect(r))
        }.mapWithState(groupByFunc)

        joinType match {
          case JoinType.innerJoin =>
            val joined = leftStream.stateSnapshots().join(rightStream.stateSnapshots())
            val results = joined.flatMap {
              case (key, (left, right)) =>
                Row.crossProduct(left, right)
            }
            new DStreamData(results, option, true)
          case JoinType.leftJoin =>
            val joined = leftStream.stateSnapshots().leftOuterJoin(rightStream.stateSnapshots())
            val results = joined.flatMap {
              case (key, (left, rightOpt)) =>
                var right = optToRow(rightOpt, Seq(Row.empty(rightSelect.card)))
                Row.crossProduct(left, right)
            }
            new DStreamData(results, option, true)
          case JoinType.rightJoin =>
            val joined = leftStream.stateSnapshots().rightOuterJoin(rightStream.stateSnapshots())
            val results = joined.flatMap {
              case (key, (leftOpt, right)) =>
                var left = optToRow(leftOpt, Seq(Row.empty(leftSelect.card)))
                Row.crossProduct(left, right)
            }
            new DStreamData(results, option, true)
          case JoinType.fullJoin =>
            val joined = leftStream.stateSnapshots().fullOuterJoin(rightStream.stateSnapshots())
            val results = joined.flatMap {
              case (key, (leftOpt, rightOpt)) =>
                var left = optToRow(leftOpt, Seq(Row.empty(leftSelect.card)))
                var right = optToRow(rightOpt, Seq(Row.empty(rightSelect.card)))
                Row.crossProduct(left, right)
            }
            new DStreamData(results, option, true)

        }
      case _ =>
        throw new IllegalArgumentException("Can only join with DStreamData")
    }
  }
}

object DStreamData {
  def optToRow[Row](opt: Option[Seq[Row]], default: Seq[Row]) = opt match {
    case None => default
    case Some(rows) => rows
  }

  def distinctFunc(batchTime: Time, key: Row, value: Option[Row], state: State[Row]): Option[Row] = {
    val stateOpts = state.getOption()
    if (stateOpts.isEmpty) {
      if (!value.isEmpty) {
        val row = value.get
        val newRow = row // new RowWithKey(row.data, key)
        state.update(newRow)
        Some(newRow)
      } else {
        None
      }
    } else Some(stateOpts.get)
  }

  def reduceFunc(reduce: (Row, Row) => Row)(batchTime: Time, key: Row, value: Option[Row], state: State[Row]): Option[Row] = {
    val last = (value, state.getOption()) match {
      case (Some(a), Some(b)) => reduce(a, b) //new RowWithKey(reduce(a, b).data, key)
      case (None, Some(b)) => b
      case (Some(a), _) => a //new RowWithKey(a.data, key)
    }
    state.update(last)
    Some(last)
  }

  def groupFunc(batchTime: Time, key: Row, value: Option[Row], state: State[Seq[Row]]): Option[Seq[Row]] = {
    val last: Seq[Row] = (value, state.getOption()) match {
      case (Some(a), Some(b)) => Seq(a) ++ b
      case (None, Some(b)) => b
      case (Some(a), _) => Seq(a)
    }
    state.update(last)
    Some(last)
  }
}

class DStreamTable[R: ClassTag](schema: Schema, streamData: DStreamData[R]) extends StreamingTable[R](schema, streamData) {
  override def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T] = {
    new DStreamTable[T](newSchema, rowBased.asInstanceOf[DStreamData[T]])
  }

  override def compile(stmt: Statement): Executable[Table] = new StreamingStatementCompiler(this).compile(stmt, schema)

  override def collectAsQueue[T: ClassTag](): mutable.Queue[T] = streamData.asQueue[T]

  override def collectAsRowQueue(): mutable.Queue[Row] = streamData.asRowQueue

}

object DStreamTable {
  def apply[R: ClassTag](schema: Schema, dstream: DStream[R]): DStreamTable[R] = new DStreamTable[R](schema, new DStreamData(dstream))
}

class Reference[T] extends Serializable {
  var value: T = null.asInstanceOf[T]
  def set(v: T) = synchronized { value = v }
  def get(): T = synchronized { value }
}

class DStreamSnapshotCollector[R: ClassTag](dstream: DStream[R]) extends SnapshotCollector[R] {
  import scala.collection.JavaConverters._
  val buffers = util.Collections.synchronizedMap(new util.LinkedHashMap[Any, R]())

  DStreamSnapshotCollector.rddCollect[R](dstream, buffers)

  override def collect: List[R] = buffers.values.asScala.toList
}

object DStreamSnapshotCollector {
  def rddCollect[T](stream: DStream[T], buffers: util.Map[Any, T]) = {
    var i = 0
    stream.foreachRDD {
      rdd: RDD[T] =>
        rdd.collect.foreach {
          case r: RowWithKey =>
            r.normalize
            buffers.put(r.key, r.asInstanceOf[T])
          case t: Row =>
            buffers.put(i, t.normalize.asInstanceOf[T])
            i += 1
        }
    }
  }

  def accumulativeCollect[T](stream: DStream[T], buffers: util.Map[Any, T]) = {
    var i = 0
    stream.foreachRDD {
      rdd =>
        rdd.collect.foreach {
          r =>
            buffers.put(i, r)
            i += 1
        }
    }
  }

  def snapshotCollect[T](stream: DStream[T], ref: Reference[List[T]]): List[T] = {
    var buffers: List[T] = null
    stream.foreachRDD {
      rdd =>
        val collected = rdd.collect
        ref.set(collected.toList)
    }
    buffers
  }
}

class SnapshotStreamCollector[R: ClassTag](stream: DStream[R]) extends SnapshotCollector[R] {

  var buffers: Reference[List[R]] = new Reference[List[R]]

  DStreamSnapshotCollector.snapshotCollect(stream, buffers)

  override def collect: List[R] = buffers.get
}

class AccumulativeCollector[R: ClassTag](dstream: DStream[R]) extends SnapshotCollector[R] {
  import scala.collection.JavaConverters._
  var i = 0
  val buffers = util.Collections.synchronizedMap(new util.LinkedHashMap[Any, R]())

  DStreamSnapshotCollector.accumulativeCollect(dstream, buffers)

  override def collect: List[R] = buffers.values.asScala.toList
}
