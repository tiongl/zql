package zql.flink

import java.lang.Iterable

import org.apache.flink.api.common.functions.{ MapFunction, FlatMapFunction, GroupCombineFunction }
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.{ BasicTypeInfo, TypeInformation }
import org.apache.flink.api.scala.DataSet
import org.apache.flink.util.Collector
import zql.core._
import zql.rowbased._
import zql.schema.Schema
import zql.util.Utils

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class FlinkData[ROW: ClassTag](val ds: DataSet[ROW], val option: CompileOption = new CompileOption) extends RowBasedData[ROW] {
  override def withOption(opt: CompileOption): RowBasedData[ROW] = new FlinkData[ROW](ds, opt)

  val stmtInfo = if (option.contains("stmtInfo")) option("stmtInfo").asInstanceOf[StatementInfo] else null
  val resultSchema = if (option.contains("resultSchema")) option("resultSchema").asInstanceOf[Schema] else null

  implicit def dsToData[T: ClassTag](newDs: DataSet[T]) = new FlinkData[T](newDs, option)

  implicit val rowTypeInfo = new BasicRowTypeInfo

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
    val func = new GroupCombineFunction[ROW, Row] {
      override def combine(values: Iterable[ROW], out: Collector[Row]): Unit = {
        val outputRow = values.map(valueFunc(_)).reduce(groupFunc(_, _))
        out.collect(outputRow)
      }
    }
    ds.groupBy(keyFunc).combineGroup(func)
  }

  override def crossJoin[T: ClassTag](other: RowBasedData[T], leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T]): RowBasedData[Row] = {
    val otherDs = other.asInstanceOf[FlinkData[T]].ds
    val joined = this.asInstanceOf[FlinkData[(ROW, T)]].ds.join(otherDs)
    val crossProduct = ds.cross(otherDs)
    val mapFunc = new MapFunction[(ROW, T), Row] {
      override def map(value: (ROW, T)): Row = {
        val (left, right) = (value._1, value._2)
        val leftRow = leftSelect(left)
        val rightRow = rightSelect(right)
        Row.combine(leftRow, rightRow)
      }
    }
    val typeInfo = new BasicRowTypeInfo
    crossProduct.map(mapFunc)(typeInfo, scala.reflect.classTag[Row])
  }

  override def sortBy[T: ClassTag](keyFunc: (ROW) => T, ordering: Ordering[T]): RowBasedData[ROW] = {
    implicit val keyTypeInfo = createTypeInfo(stmtInfo.stmt.orderBy.map(_.dataType)).asInstanceOf[TypeInformation[T]]
    //TODO; This is just partially working as apache flink doesn't support global sort
    ds.partitionByRange(keyFunc).sortPartition(keyFunc, Order.ASCENDING)
  }

  override def joinWithKey[T: ClassTag](
    other: RowBasedData[T],
    leftKeyFunc: RowBuilder[ROW], rightKeyFunc: RowBuilder[T],
    leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T],
    joinType: JoinType
  ): RowBasedData[Row] = other match {
    case rightData: FlinkData[T] =>
      //TODO: Check whether empty key func would cause single hotspot
      val outFunc = (a: ROW, b: T) => {
        val left = if (a == null) Row.empty(leftSelect.card) else leftSelect(a)
        val right = if (b == null) Row.empty(rightSelect.card) else rightSelect(b)
        Row.combine(left, right)
      }

      joinType match {
        case JoinType.innerJoin =>
          val joinedDataset = ds.join(rightData.ds).where(leftKeyFunc(_)).equalTo(rightKeyFunc(_))
          val results = joinedDataset.map[Row] {
            t: (ROW, T) =>
              val left = leftSelect(t._1)
              val right = rightSelect(t._2)
              Row.combine(left, right)
          }
          results
        case JoinType.leftJoin =>
          ds.leftOuterJoin(rightData.ds).where(leftKeyFunc(_)).equalTo(rightKeyFunc(_)).apply(outFunc)
        case JoinType.rightJoin =>
          ds.rightOuterJoin(rightData.ds).where(leftKeyFunc(_)).equalTo(rightKeyFunc(_)).apply(outFunc)
        case JoinType.fullJoin =>
          ds.fullOuterJoin(rightData.ds).where(leftKeyFunc(_)).equalTo(rightKeyFunc(_)).apply(outFunc)

      }
    case _ =>
      throw new IllegalArgumentException("Unsupported join type " + other.toString)
  }
}

class FlinkDsTable[T: ClassTag](schema: Schema, dsData: FlinkData[T]) extends RowBasedTable[T](schema) {
  override def data: RowBasedData[T] = dsData

  override def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T] = new FlinkDsTable[T](newSchema, rowBased.asInstanceOf[FlinkData[T]])

}

object FlinkDsTable {
  class ROWFUNC[ROW, B: ClassTag](func: (ROW) => B)

  def apply[ROW: ClassTag](schema: Schema, data: DataSet[ROW]) = {
    new FlinkDsTable[ROW](schema, new FlinkData(data))
  }
}
