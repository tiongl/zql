package zql.spark

import org.apache.spark.rdd.RDD
import zql.core
import zql.core._
import zql.rowbased._
import zql.schema.Schema
import zql.util.Utils
import scala.reflect.ClassTag

class RDDData[ROW: ClassTag](val rdd: RDD[ROW], val option: CompileOption = new CompileOption()) extends RowBasedData[ROW] {

  implicit def rddToRDDData[T: ClassTag](rdd: RDD[T]) = new RDDData(rdd, option)

  //This is slow as we zipWithIndex to filter the rows we need.
  override def slice(offset: Int, untilN: Int) = rdd.zipWithIndex().filter(t => t._2 >= offset && t._2 < untilN).map(_._1)

  override def reduce(reduceFunc: (ROW, ROW) => ROW): RowBasedData[ROW] = {
    val reduce = rdd.reduce(reduceFunc)
    rdd.sparkContext.parallelize[ROW](Seq(reduce), 1)
  }

  override def filter(filter: (ROW) => Boolean): RowBasedData[ROW] = rdd.filter(filter)

  override def groupBy(keyFunc: (ROW) => Row, valueFunc: (ROW) => Row, aggFunc: (Row, Row) => Row): RowBasedData[Row] = {
    val tuples = rdd.map { row => (keyFunc(row), valueFunc(row)) }
    tuples.reduceByKey(aggFunc).map(_._2)
  }

  override def size: Int = ???

  override def sortBy[T: ClassTag](keyFunc: (ROW) => T, ordering: Ordering[T]): RowBasedData[ROW] = {
    rdd.sortBy(keyFunc)(ordering, scala.reflect.classTag[T])
  }

  override def asList[T]: List[T] = rdd.collect().toList.asInstanceOf[List[T]]

  override def map[T: ClassTag](mapFunc: (ROW) => T): RowBasedData[T] = rdd.map(mapFunc)

  override def isLazy = true

  override def withOption(option: CompileOption): RowBasedData[ROW] = new RDDData[ROW](rdd, option)

  override def distinct(): RowBasedData[ROW] = rdd.distinct()

  override def crossJoin[T: ClassTag](other: RowBasedData[T], leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T]): RowBasedData[Row] = other match {
    case rddData: RDDData[T] =>
      rdd.cartesian(rddData.rdd).map {
        case (r1, r2) =>
          val left = leftSelect(r1)
          val right = rightSelect(r2)
          Row.combine(left, right)
      }
    case _ =>
      throw new IllegalArgumentException("Unsupported join type " + other.toString)
  }

  override def joinWithKey[T: ClassTag](
    other: RowBasedData[T],
    leftKeyFunc: RowBuilder[ROW], rightKeyFunc: RowBuilder[T],
    leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T],
    joinType: JoinType
  ): RowBasedData[Row] = {

    other match {
      case rightData: RDDData[T] =>
        //TODO: Check whether empty key func would cause single hotspot
        val left = rdd.groupBy(leftKeyFunc)
        val right = rightData.rdd.groupBy(rightKeyFunc)
        joinType match {
          case JoinType.innerJoin =>
            val joined = left.join(right)
            joined.map(_._2).flatMap {
              case (leftList, rightList) =>
                val leftSelected = leftList.map(leftSelect(_))
                val rightSelected = rightList.map(rightSelect(_))
                Row.crossProduct(leftSelected, rightSelected).seq
            }
          case JoinType.leftJoin =>
            val joined = left.leftOuterJoin(right)
            joined.map(_._2).flatMap {
              case (leftList, rightListOpt) =>
                val leftSelected = leftList.map(leftSelect(_))
                val rightSelected = RDDData.checkRowsOrReturnEmpty(rightListOpt, rightSelect)
                Row.crossProduct(leftSelected, rightSelected).seq
            }
          case JoinType.rightJoin =>
            val joined = left.rightOuterJoin(right)
            joined.map(_._2).flatMap {
              case (leftListOpt, rightList) =>
                val rightSelected = rightList.map(rightSelect(_))
                val leftSelected = RDDData.checkRowsOrReturnEmpty(leftListOpt, leftSelect)
                Row.crossProduct(leftSelected, rightSelected).seq
            }
          case JoinType.fullJoin =>
            val joined = left.fullOuterJoin(right)
            joined.map(_._2).flatMap {
              case (leftListOpt, rightListOpt) =>
                val rightSelected = RDDData.checkRowsOrReturnEmpty(rightListOpt, rightSelect)
                val leftSelected = RDDData.checkRowsOrReturnEmpty(leftListOpt, leftSelect)
                Row.crossProduct(leftSelected, rightSelected).seq
            }
        }

      case _ =>
        throw new IllegalArgumentException("Unsupported join type " + other.toString)
    }
  }

}

object RDDData {
  def checkRowsOrReturnEmpty[A](rddOpt: Option[Iterable[A]], builder: RowBuilder[A]): Iterable[Row] = {
    rddOpt match {
      case None => Seq(Row.empty(builder.card))
      case Some(rdd) => rdd.map(builder(_))
    }
  }
}

class RDDTable[ROW: ClassTag](schema: Schema, rdd: RDD[ROW]) extends RowBasedTable[ROW](schema) {

  override def data: RowBasedData[ROW] = new RDDData(rdd)

  override def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): RowBasedTable[T] = {
    val rdd = rowBased.asInstanceOf[RDDData[T]].rdd
    new RDDTable[T](newSchema, rdd)
  }
}
