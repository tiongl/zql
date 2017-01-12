package zql.flink

import java.lang.Iterable

import org.apache.flink.api.common.functions.{ FlatMapFunction, GroupCombineFunction }
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.{ BasicTypeInfo, TypeInformation }
import org.apache.flink.api.scala.DataSet
import org.apache.flink.util.Collector
import zql.core._
import zql.rowbased.{ Row, RowBasedTable, RowBasedData }
import zql.schema.Schema

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class FlinkData[ROW: ClassTag](val ds: DataSet[ROW], val option: CompileOption = new CompileOption) extends RowBasedData[ROW] {
  override def withOption(opt: CompileOption): RowBasedData[ROW] = new FlinkData[ROW](ds, opt)

  val stmtInfo = if (option.contains("stmtInfo")) option("stmtInfo").asInstanceOf[StatementInfo] else null
  val resultSchema = if (option.contains("resultSchema")) option("resultSchema").asInstanceOf[Schema] else null

  implicit def dsToData[T: ClassTag](newDs: DataSet[T]) = new FlinkData[T](newDs, option)

  override def isLazy: Boolean = true

  override def reduce(reduceFunc: (ROW, ROW) => ROW): RowBasedData[ROW] = ds.reduce(reduceFunc)

  override def filter(filter: (ROW) => Boolean): RowBasedData[ROW] = ds.filter(filter)

  override def distinct(): RowBasedData[ROW] = ds.distinct()

  override def join(other: RowBasedData[Row], jointPoint: (Row) => Boolean): RowBasedData[Row] = {
    val otherDs = other.asInstanceOf[FlinkData[Row]].ds
    val joined = this.asInstanceOf[FlinkData[Row]].ds.join(otherDs)
    //    stmtInfo.stmt.from.
    val crossProduct = ds.asInstanceOf[DataSet[Row]].cross(otherDs)
    val mapFunc = new FlatMapFunction[(Row, Row), Row] {
      override def flatMap(value: (Row, Row), out: Collector[Row]): Unit = {
        val r = new Row(value._1.data ++ value._2.data)
        val bool = jointPoint.apply(r)
        if (bool) out.collect(r)
      }
    }
    val typeInfo = createTypeInfo(stmtInfo.resultSchema.allColumns.map(_.dataType))
    crossProduct.flatMap { mapFunc }(typeInfo, scala.reflect.classTag[Row])
  }

  def createTypeInfo(types: Seq[Class[_]]): TypeInformation[Row] = {
    val typeInfos = types.map {
      case i: Class[Int] =>
        BasicTypeInfo.INT_TYPE_INFO
      case s: Class[Short] =>
        BasicTypeInfo.SHORT_TYPE_INFO
      case s: Class[String] =>
        BasicTypeInfo.STRING_TYPE_INFO
    }
    new RowTypeInfo(typeInfos: _*)
  }

  override def groupBy(keyFunc: (ROW) => Row, valueFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row] = {
    implicit val seqKeyTypeInfo = createTypeInfo(stmtInfo.stmt.groupBy.map(_.dataType))
    implicit val rowTypeInfo = createTypeInfo(stmtInfo.expandedSelects.map(_.dataType))
    val func = new GroupCombineFunction[ROW, Row] {
      override def combine(values: Iterable[ROW], out: Collector[Row]): Unit = {
        val outputRow = values.map(valueFunc(_)).reduce(_.aggregate(_, aggregatableIndices))
        out.collect(outputRow)
      }
    }
    ds.groupBy(keyFunc)(seqKeyTypeInfo).combineGroup(func)(rowTypeInfo, scala.reflect.classTag[Row])

  }

  override def size: Int = ds.size

  override def sortBy(keyFunc: (ROW) => Row, ordering: Ordering[Row], tag: ClassManifest[Row]): RowBasedData[ROW] = {
    implicit val keyTypeInfo = createTypeInfo(stmtInfo.stmt.orderBy.map(_.dataType))
    //    ds.setParallelism(1).sortPartition(keyFunc, Order.ASCENDING)
    ds.partitionByRange(keyFunc).sortPartition(keyFunc, Order.ASCENDING)
  }

  override def slice(offset: Int, until: Int): RowBasedData[ROW] = if (offset == 0) ds.first(until) else throw new UnsupportedOperationException()

  override def asList[T]: List[T] = ds.collect().toList.asInstanceOf[List[T]]

  override def map[T: ClassTag](mapFunc: (ROW) => T): RowBasedData[T] = {
    implicit val rowTypeInfo = createTypeInfo(stmtInfo.expandedSelects.map(_.dataType)).asInstanceOf[TypeInformation[T]]
    ds.map(mapFunc)
  }
}

class FlinkDsTable[T: ClassTag](schema: Schema, dsData: FlinkData[T]) extends RowBasedTable[T](schema) {
  override def data: RowBasedData[T] = dsData

  override def createTable[T: ClassManifest](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T] = new FlinkDsTable[T](newSchema, rowBased.asInstanceOf[FlinkData[T]])

}

object FlinkDsTable {
  class ROWFUNC[ROW, B: ClassTag](func: (ROW) => B)

  def apply[ROW: ClassTag](schema: Schema, data: DataSet[ROW]) = {
    new FlinkDsTable[ROW](schema, new FlinkData(data))
  }
}
