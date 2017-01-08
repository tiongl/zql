package zql.rowbased

import zql.core.Aggregatable
import zql.util.Utils

case class Row(val data: Array[Any]) extends Comparable[Row] {

  def aggregate(row: Row, indices: Array[Int]): Row = {
    //TODO: make sure this won't have side effect as we use shallow copy
    val newRow = new Row(data)
    indices.foreach { i =>
      val a = data(i).asInstanceOf[Aggregatable[Any]]
      val b = row.data(i).asInstanceOf[Aggregatable[Any]]
      newRow.data(i) = a.aggregate(b)
    }
    newRow
  }

  def normalize: Row = {
    data.zipWithIndex.map {
      case (value, index) =>
        value match {
          case s: Aggregatable[_] =>
            data(index) = s.value
          case any: AnyRef =>
            any
        }
    }
    this
  }

  override lazy val hashCode = data.map(_.hashCode()).sum

  override def equals(obj: scala.Any): Boolean = if (obj == null) false else obj.asInstanceOf[Row].data.sameElements(data)

  override def toString = data.mkString(",")

  override def compareTo(o: Row): Int = Row.rowOrdering.compare(this, o)
}

object Row {
  implicit val rowOrdering = new RowOrdering()
  implicit val EMPTY_ROW = new Row(Array()) {
    override def aggregate(row: Row, indices: Array[Int]): Row = row
  }
}

class EmptyRow(array: Array[Any]) extends Row(array) {
  override def aggregate(row: Row, indices: Array[Int]): Row = row
}

class RowOrdering(ascendings: Array[Boolean] = Array()) extends Ordering[Row] {
  override def compare(x: Row, y: Row): Int = {
    x.data.zip(y.data).zipWithIndex.foreach {
      case ((a, b), index) =>
        val comparedValue = Utils.compare(a, b)
        if (comparedValue != 0) {
          if (ascendings.length <= index || ascendings(index)) {
            return comparedValue
          } else {
            return comparedValue * -1
          }

        }
    }
    0
  }
}

