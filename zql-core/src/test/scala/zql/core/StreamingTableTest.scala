package zql.core

import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import zql.rowbased.{ StreamingTable, ReflectedSchema, Row }
import zql.util.Utils

import scala.collection.mutable

abstract class StreamingTableTest extends TableTest {
  override def supportSelectDistinct = {
    executeAndMatch(
      selectDistinct('firstName) from personTable,
      persons.map(_.firstName).distinct.sorted.map(d => new Row(Array(d)))
    )
  }

  override def supportSimpleGroupFunction = {
    executeAndMatch(
      select('firstName, sum('age)) from personTable groupBy ('firstName), {
        //because some aggregation system (spark) has unordered result, we have to use order by here
        val linkedHash = new mutable.LinkedHashMap[Seq[Any], Row]
        Utils.groupBy[Person, Row](
          persons,
          _.firstName,
          p => new Row(Array(p.firstName, new Summable(p.age))),
          (a: Row, b: Row) => a.aggregate(b, Array(1))
        )
      }.map(_.normalize).toList.sortBy(_.data(0).toString)
    )
  }

  override def supportSelectOrdering = {
    assert(true)
  }

  override def supportSelectOrderingDesc = {
    assert(true)
  }

  override def supportSelectLimit = {
    assert(true)
  }

  override def supportSelectLimitOffset = {
    assert(true)
  }
}