package zql.spark

import java.util
import java.util.concurrent.{ LinkedBlockingDeque, BlockingQueue }

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import zql.core.{ Table, Executable, Statement, CompileOption }
import zql.rowbased._
import zql.schema.Schema

import scala.collection.mutable
import scala.reflect.ClassTag

class DStreamData[R: ClassTag](val stream: DStream[R], val option: CompileOption = new CompileOption) extends StreamData[R] {

  implicit def streamToStreamData[T: ClassTag](stream: DStream[T]) = new DStreamData(stream, option)

  override def isLazy: Boolean = true

  override def reduce(reduceFunc: (R, R) => R): RowBasedData[R] = {
    val groupByFunc = DStreamData.toGroupbyFunc(Array[Int]())
    stream.asInstanceOf[DStream[Row]].map(d => (Row.EMPTY_ROW, d)).updateStateByKey { groupByFunc }.map(DStreamData.rowToKeyRow(_)).asInstanceOf[DStream[R]]
  }

  override def filter(filter: (R) => Boolean): RowBasedData[R] = stream.filter(filter)

  override def distinct(): RowBasedData[R] = {
    val distinctFunc = DStreamData.toDistinctFunc()
    stream.asInstanceOf[DStream[Row]].map(r => (r, r)).updateStateByKey { distinctFunc }.map { DStreamData.rowToKeyRow(_) }.asInstanceOf[DStream[R]]
  }

  override def withOption(option: CompileOption): RowBasedData[R] = new DStreamData(stream, option)

  override def join(other: RowBasedData[Row], jointPoint: (Row) => Boolean): RowBasedData[Row] = ???

  override def size: Int = ???

  override def groupBy(keyFunc: (R) => Row, selectFunc: (R) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row] = {
    val groupByFunc = DStreamData.toGroupbyFunc(aggregatableIndices)
    stream.map(t => (keyFunc(t), selectFunc(t))).updateStateByKey { groupByFunc }.map { DStreamData.rowToKeyRow(_) }
  }

  override def sortBy(keyFunc: (R) => Row, ordering: Ordering[Row], tag: ClassManifest[Row]): RowBasedData[R] = this

  override def slice(offset: Int, until: Int): RowBasedData[R] = ???

  override def asList[T]: List[T] = {
    stream.foreachRDD {
      rdd =>
        println(rdd.collect().mkString(","))
    }
    List()
  }

  override def asQueue[T: ClassTag]: mutable.Queue[T] = {
    val queue = mutable.Queue[T]()
    stream.foreachRDD {
      rdd =>
        queue.enqueue(rdd.collect().toSeq.asInstanceOf[Seq[T]]: _*)
    }
    queue
  }

  override def getSnapshotCollector: SnapshotCollector[R] = {
    new DStreamSnapshotCollecotor[R](stream)
  }

  override def asRowQueue = {
    val queue = mutable.Queue[Row]()
    stream.foreachRDD {
      rdd =>
        queue.enqueue(rdd.collect().toSeq.asInstanceOf[Seq[Row]]: _*)
    }
    queue
  }

  override def map[T: ClassTag](mapFunc: (R) => T): RowBasedData[T] = stream.map(mapFunc(_))
}

object DStreamData {
  def toGroupbyFunc(aggregateIndices: Array[Int]) = {
    (rows: Seq[Row], out: Option[Row]) =>
      {
        println("Checking " + out + " with " + rows.mkString(", "))
        val result = out match {
          case None =>
            rows.reduce(_.aggregate(_, aggregateIndices))
          case Some(r) => (rows ++ Seq(r)).reduce(_.aggregate(_, aggregateIndices))
        }
        Option(result)
      }
  }

  def rowToKeyRow(r: (Row, Row)) = {
    new RowWithKey(r._2.data, r._1).asInstanceOf[Row]
  }

  def toDistinctFunc() = {
    (rows: Seq[Row], out: Option[Row]) =>
      {
        val result = out match {
          case None => rows.head
          case Some(r) => r
        }
        Option(result)
      }
  }
}

class DStreamTable[R: ClassTag](schema: Schema, streamData: DStreamData[R]) extends StreamingTable[R](schema, streamData) {
  override def createTable[T: ClassManifest](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T] = new DStreamTable[T](newSchema, rowBased.asInstanceOf[DStreamData[T]])

  override def compile(stmt: Statement): Executable[Table] = new StreamingStatementCompiler(this).compile(stmt, schema)

  override def collectAsQueue[T: ClassTag](): mutable.Queue[T] = streamData.asQueue[T]

  override def collectAsRowQueue(): mutable.Queue[Row] = streamData.asRowQueue

}

object DStreamTable {
  def apply[R: ClassTag](schema: Schema, dstream: DStream[R]): DStreamTable[R] = new DStreamTable[R](schema, new DStreamData(dstream))
}

class DStreamSnapshotCollecotor[R: ClassTag](dstream: DStream[R]) extends SnapshotCollector[R] {
  import scala.collection.JavaConverters._
  val buffers = util.Collections.synchronizedMap(new util.LinkedHashMap[Any, R]())

  DStreamSnapshotCollecotor.rddCollect(dstream, buffers)

  override def collect: List[R] = buffers.values.asScala.toList
}

object DStreamSnapshotCollecotor {
  def rddCollect[T](stream: DStream[T], buffers: util.Map[Any, T]) = {
    var i = 0
    stream.foreachRDD {
      rdd: RDD[T] =>
        rdd.collect.foreach {
          case r: RowWithKey =>
            buffers.put(r.key, r.asInstanceOf[T])
          case t: T =>
            buffers.put(i, t)
            i += 1
        }
    }
  }
}