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

  override def size: Int = ds.size

  override def slice(offset: Int, until: Int): RowBasedData[ROW] = if (offset == 0) ds.first(until) else throw new UnsupportedOperationException()

  override def asList[T]: List[T] = ds.collect().toList.asInstanceOf[List[T]]

  override def map[T: ClassTag](mapFunc: (ROW) => T): RowBasedData[T] = {
    implicit val rowTypeInfo = createTypeInfo(stmtInfo.expandedSelects.map(_.dataType)).asInstanceOf[TypeInformation[T]]
    ds.map(mapFunc)
  }

  //  def groupBy(keyFunc: (ROW) => Row, selectFunc: (ROW) => Row, aggregatableIndices: Array[Int]): RowBasedData[Row]
  override def groupBy(keyFunc: (ROW) => Row, valueFunc: (ROW) => Row, groupFunc: (Row, Row) => Row): RowBasedData[Row] = {
    implicit val seqKeyTypeInfo = createTypeInfo(stmtInfo.stmt.groupBy.map(_.dataType))
    implicit val rowTypeInfo = createTypeInfo(stmtInfo.expandedSelects.map(_.dataType))
    val func = new GroupCombineFunction[ROW, Row] {
      override def combine(values: Iterable[ROW], out: Collector[Row]): Unit = {
        val outputRow = values.map(valueFunc(_)).reduce(groupFunc(_, _))
        out.collect(outputRow)
      }
    }
    ds.groupBy(keyFunc)(seqKeyTypeInfo).combineGroup(func)(rowTypeInfo, scala.reflect.classTag[Row])
  }

  override def join[T: ClassManifest](other: RowBasedData[T], jointPoint: (Row) => Boolean, rowifier: (ROW, T) => Row): RowBasedData[Row] = {
    val otherDs = other.asInstanceOf[FlinkData[T]].ds
    val joined = this.asInstanceOf[FlinkData[(ROW, T)]].ds.join(otherDs)
    //    stmtInfo.stmt.from.
    val crossProduct = ds.cross(otherDs)
    val mapFunc = new FlatMapFunction[(ROW, T), Row] {
      override def flatMap(value: (ROW, T), out: Collector[Row]): Unit = {
        val r = rowifier(value._1, value._2)
        val bool = jointPoint.apply(r)
        if (bool) out.collect(r)
      }
    }
    val typeInfo = createTypeInfo(stmtInfo.resultSchema.allColumns.map(_.dataType))
    crossProduct.flatMap { mapFunc }(typeInfo, scala.reflect.classTag[Row])
  }

  override def sortBy[T: ClassManifest](keyFunc: (ROW) => T, ordering: Ordering[T]): RowBasedData[ROW] = {
    implicit val keyTypeInfo = createTypeInfo(stmtInfo.stmt.orderBy.map(_.dataType)).asInstanceOf[TypeInformation[T]]
    //    ds.setParallelism(1).sortPartition(keyFunc, Order.ASCENDING)
    ds.partitionByRange(keyFunc).sortPartition(keyFunc, Order.ASCENDING)
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
