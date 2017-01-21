package zql.list

import zql.core._
import zql.rowbased._
import zql.schema.Schema
import zql.util.Utils

import scala.reflect.ClassTag

class ListTable[ROW: ClassTag](schema: Schema, list: List[ROW]) extends RowBasedTable[ROW](schema) {

  val data = new ListData(list)

  override def createTable[T: ClassTag](newSchema: Schema, rowBased: RowBasedData[T]): ListTable[T] = {
    val list = rowBased.asInstanceOf[ListData[T]].list
    new ListTable(newSchema, list)
  }

  override def as(alias: Symbol): Table = new ListTable[ROW](schema.as(alias), list)
}

class ListData[ROW: ClassTag](val list: List[ROW], val option: CompileOption = new CompileOption) extends RowBasedData[ROW] {

  implicit def listToListData[T: ClassTag](list: List[T]) = new ListData[T](list, option)

  override def filter(filter: (ROW) => Boolean): RowBasedData[ROW] = list.filter(filter)

  override def groupBy(keyFunc: (ROW) => Row, valueFunc: (ROW) => Row, aggregateFunc: (Row, Row) => Row): RowBasedData[Row] = {
    Utils.groupBy[ROW, Row](list, keyFunc(_), valueFunc(_), aggregateFunc).toList
  }

  override def reduce(reduceFunc: (ROW, ROW) => ROW) = List(list.reduce(reduceFunc))

  override def map[T: ClassTag](mapFunc: (ROW) => T) = list.map(mapFunc)

  override def sortBy[T: ClassTag](keyFunc: (ROW) => T, ordering: Ordering[T]) = new ListData(list.sortBy(keyFunc)(ordering))

  override def slice(offset: Int, until: Int) = list.slice(offset, until)

  override def size() = list.length

  override def asList[T] = list.asInstanceOf[List[T]]

  override def isLazy = false

  override def withOption(opt: CompileOption): RowBasedData[ROW] = new ListData(list, opt)

  override def distinct() = list.distinct

  override def crossJoin[T: ClassTag](other: RowBasedData[T], leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T]): RowBasedData[Row] = {
    other match {
      case rbd: ListData[T] =>
        val newList = list.flatMap {
          t1 =>
            rbd.list.map {
              t2 =>
                val t1Row = leftSelect(t1)
                val t2Row = rightSelect(t2)
                Row.combine(t1Row, t2Row)
            }
        }
        newList
      case _ =>
        throw new IllegalArgumentException("Joining with " + other.getClass + " is not supported")
    }
  }

  override def joinWithKey[T: ClassTag](
    other: RowBasedData[T],
    leftKeyFunc: RowBuilder[ROW], rightKeyFunc: RowBuilder[T],
    leftSelect: RowBuilder[ROW], rightSelect: RowBuilder[T],
    joinType: JoinType
  ): RowBasedData[Row] = {

    def listOrElse(rowsOpt: Option[Seq[Row]], noneResult: Seq[Row]) = rowsOpt match {
      case None => noneResult
      case Some(rows) => rows
    }

    other match {
      case rhs: ListData[T] =>
        val leftMap = Utils.groupBy[ROW, Row, Row](list, leftKeyFunc, leftSelect)
        val rightMap = Utils.groupBy[T, Row, Row](rhs.list, rightKeyFunc, rightSelect)
        joinType match {
          case JoinType.innerJoin =>
            val allRows = leftMap.flatMap {
              case (leftkey, leftRows) =>
                val rightRows = listOrElse(rightMap.get(leftkey), Seq())
                Row.crossProduct(leftRows, rightRows)
            }
            allRows.toList
          case JoinType.leftJoin =>
            val allRows = leftMap.flatMap {
              case (leftkey, leftRows) =>
                val rightRows = listOrElse(rightMap.get(leftkey), Seq(Row.empty(rightSelect.card)))
                Row.crossProduct(leftRows, rightRows)
            }
            allRows.toList
          case JoinType.rightJoin =>
            val allRows = rightMap.flatMap {
              case (rightKey, rightRows) =>
                val leftRows = listOrElse(leftMap.get(rightKey), Seq(Row.empty(leftSelect.card)))
                Row.crossProduct(leftRows, rightRows)
            }
            allRows.toList
          case JoinType.fullJoin =>
            val allLeftRows = leftMap.flatMap {
              case (leftkey, leftRows) =>
                val rightRows = listOrElse(rightMap.get(leftkey), Seq(Row.empty(rightSelect.card)))
                Row.crossProduct(leftRows, rightRows)
            }
            val remainKeys = rightMap.keySet -- leftMap.keySet
            val remainRows = remainKeys.flatMap {
              rightKey =>
                val rightRows = rightMap(rightKey)
                val leftRows = Seq(Row.empty(leftSelect.card))
                Row.crossProduct(leftRows, rightRows)
            }
            (allLeftRows ++ remainRows).toList
        }
      case _ =>
        throw new IllegalArgumentException("Joining with " + other.getClass + " is not supported")
    }
  }
}