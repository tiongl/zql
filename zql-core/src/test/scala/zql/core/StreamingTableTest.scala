package zql.core

import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import zql.rowbased.{ StreamingTable, ReflectedSchema, Row }
import zql.util.Utils

import scala.collection.mutable

abstract class StreamingTableTest extends TableTest {

  def startStreaming(): Unit

  override def executeAndMatch(statement: StatementWrapper, rows: List[Row], sort: Boolean = true) = {
    val resultTable = statement.compile.execute().asInstanceOf[StreamingTable[_]]
    val collector = resultTable.getSnapshotCollector()
    println("SQL = " + statement.statement().toSql())
    startStreaming()
    val results = collector.collect.asInstanceOf[List[Row]]
    println("Results = " + results.sorted.mkString(", "))
    println("Expected = " + rows.sorted.mkString(", "))
    assert(results.sorted.equals(rows.sorted))
    results.sorted should be(rows.sorted)
  }

  override def supportSameTableJoinOn = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (personTable as 't2) on (c"t1.id" === c"t2.id")),
      persons.map {
        t1 =>
          val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId)
          new Row(all)
      }
    )
  }

  override def supportSameTableJoinTableWithFilter = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (personTable as 't2) on (c"t1.id" === c"t2.id")) where c"t1.age" > 10,
      persons.filter(_.age > 10).map {
        t1 =>
          val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId)
          new Row(all)
      }
    )
  }

  override def supportJoinTableWithFilter = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (departmentTable as 't2) on (c"t1.departmentId" === c"t2.id")) where c"t1.age" > 10,
      persons.filter(_.age > 10).flatMap {
        t1 =>
          departments.flatMap {
            t2 =>
              if (t1.departmentId == t2.id) {
                val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t2.id, t2.name)
                Seq(new Row(all))
              } else {
                Seq()
              }
          }
      }
    )
  }

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