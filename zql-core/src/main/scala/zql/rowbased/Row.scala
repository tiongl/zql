package zql.rowbased

import zql.core.Aggregatable
import zql.util.Utils

case class Row(val data: Array[Any] = Array()) extends Comparable[Row] {

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
    val newData = data.zipWithIndex.map {
      case (value, index) =>
        value match {
          case s: Aggregatable[_] =>
            s.value
          case any: AnyRef =>
            any
        }
    }
    new Row(newData)
  }

  override lazy val hashCode = data.map(d => if (d == null) 0 else d.hashCode()).sum

  override def equals(obj: scala.Any): Boolean = if (obj == null) false else obj.asInstanceOf[Row].data.sameElements(data)

  override def toString = data.mkString(",")

  override def compareTo(o: Row): Int = Row.rowOrdering.compare(this, o)
}

class RowWithKey(data: Array[Any], val key: Row) extends Row(data) {
  override def aggregate(row: Row, indices: Array[Int]): Row = {
    val other = row.asInstanceOf[RowWithKey]
    val r = super.aggregate(row, indices)
    new RowWithKey(r.data, key)
  }

  override def normalize: Row = {
    val nRow = super.normalize
    new RowWithKey(nRow.data, key)
  }

  override def toString = {
    super.toString + " withkey " + key.toString
  }
}

object Row {
  implicit val rowOrdering = new RowOrdering()
  implicit val EMPTY_ROW = new Row(Array()) {
    override def aggregate(row: Row, indices: Array[Int]): Row = row
  }

  def empty(num: Int) = {
    new Row(new Array(num))
  }

  def combine(a: Row, b: Row) = {
    new Row(a.data ++ b.data)
  }

  def crossProduct(lhs: Iterable[Row], rhs: Iterable[Row]): Iterable[Row] = {
    lhs.flatMap {
      left =>
        rhs.map {
          right => Row.combine(left, right)
        }
    }
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

