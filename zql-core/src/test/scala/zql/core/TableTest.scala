package zql.core

import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import zql.core.util.Utils
import scala.collection.mutable

abstract class TableTest extends FlatSpec with Matchers with BeforeAndAfterAll with PersonExample {

  def table: Table

  def executeAndMatch(statement: Compilable, rows: List[Row]) = {
    val results = statement.compile.execute().collectAsList().map(r => normalizeRow(r))
    println("Results = " + results)
    //    println("rows = " + rows + rows.map(_.getClass))

    results should be(rows)
  }

  def normalizeRow(row: Any): Row = row.asInstanceOf[Row]

  //Operations
  it should "Support all operation" in supportAllOperations

  //Selects
  it should "support select with math operations" in supportSelectWithMathOperations
  it should "support select literal" in supportSelectLiteral
  it should "support select all" in supportSelectAll
  it should "support simple data column" in supportSelectDataColumn
  it should "support simple filtering" in supportSimpleFiltering
  it should "support select count" in supportSelectCount
  it should "support select count with groupby" in supportSelectCountWithGroupby
  it should "support select distinct" in supportSelectDistinct
  it should "support select count distinct" in supportSelectCountDistinct

  //Filtering
  it should "support filtering with aggregation" in supportFilteringWithAggregation
  it should "support filtering boolean logic" in supportFilteringWithBooleanLogic
  it should "support not filtering" in supportNotFilter

  //Groupby
  it should "support detect invlid aggregation" in supportDetectInvalidAggregation
  it should "validate groupby must have aggregate function" in supportDetectInvalidAggregation2
  it should "support simple groupby function" in supportSimpleGroupFunction
  it should "support groupby having " in supportGroupbyHaving

  //Ordering
  it should "support select ordering" in supportSelectOrdering
  it should "support select ordering desc" in supportSelectOrderingDesc

  //Limit
  it should "support offset, limit" in supportSelectLimitOffset
  it should "support limit" in supportSelectLimit

  //sub query
  it should "support select subquery" in supportSelectSubquery
  it should "support detect invalid subquery" in supportDetectBadSubquery
  it should "support from subquery" in supportFromSubquery

  def supportAllOperations = {
    val one = new IntLiteral(1)
    val str = new StringLiteral("test")
    executeAndMatch(
      //TODO: string + is not working
      select(one + 1, one - 1, one * 2, one / 2) from table,
      data.map(p => new Row(Array(1 + 1, 1 - 1, 1 * 2, 1 / 2.toFloat)))
    )

    //equality
    executeAndMatch(
      select(one > 1, one > 0, one > -1, one >= 1, one >= 0, one >= -1, one === 1, one === 0, one === -1, one !== 1, one !== 0, one !== -1, one < 1, one < 0, one < -1, one <= 1, one <= 0, one <= -1) from table,
      data.map(p => new Row(Array(1 > 1, 1 > 0, 1 > -1, 1 >= 1, 1 >= 0, 1 >= -1, 1 == 1, 1 == 0, 1 == -1, 1 != 1, 1 != 0, 1 != -1, 1 < 1, 1 < 0, 1 < -1, 1 <= 1, 1 <= 0, 1 <= -1)))
    )
  }

  def supportSelectWithMathOperations = {
    executeAndMatch(
      select(1 + 1, 'firstName, 'age + 1) from table,
      data.map(p => new Row(Array(2, p.firstName, p.age + 1))).toList
    )
  }

  def supportSelectLiteral = {
    executeAndMatch(
      select(1, "test", true) from table,
      data.map(p => new Row(Array(1, "test", true))).toList
    )
  }

  def supportSelectAll = {
    executeAndMatch(
      select(*) from table,
      data.map(p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.spouseId))).toList
    )
  }

  def supportSelectDataColumn = {
    executeAndMatch(
      select('firstName, 'lastName) from table,
      data.map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  def supportDetectInvalidAggregation = {
    try {
      executeAndMatch(
        select('firstName, 'lastName, sum('age)) from table,
        List()
      )
      throw new Exception("Must detect invalid aggregation statement")
    } catch {
      case e: IllegalArgumentException =>
        assert(true) //this is fine
    }
  }

  def supportSimpleFiltering = {
    executeAndMatch(
      select('firstName, 'lastName) from table where ('firstName === "John"),
      data.filter(_.firstName == "John").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  def supportFilteringWithAggregation = {
    executeAndMatch(
      select(sum('age)) from table where ('firstName === "John"),
      List(new Row(Array(data.filter(_.firstName == "John").map(_.age).sum)))
    )
  }

  def supportFilteringWithBooleanLogic = {
    executeAndMatch(
      select('firstName, 'lastName) from table where (('firstName === "John" and 'lastName === "Smith") or ('lastName === "Doe")),
      data.filter(p => (p.firstName == "John" && p.lastName == "Smith") || p.lastName == "Doe").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  def supportNotFilter = {
    executeAndMatch(
      select('firstName, 'lastName) from table where (NOT('firstName === "John")),
      data.filter(p => p.firstName != "John").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  def supportDetectInvalidAggregation2 = {
    try {
      val stmt = select('firstName, 'lastName) from table groupBy ('firstName)
      stmt.compile
      throw new Exception("Groupby without aggregate function must fail")

    } catch {
      case e: IllegalArgumentException =>
        e.getMessage should be("Group by must have at least one aggregation function")
    }
  }

  def supportSimpleGroupFunction = {
    executeAndMatch(
      select('firstName, sum('age)) from table groupBy ('firstName) orderBy ('firstName), {
        //because some aggregation system (spark) has unordered result, we have to use order by here
        val linkedHash = new mutable.LinkedHashMap[Seq[Any], Row]
        Utils.groupBy[Person, Row](
          data,
          _.firstName,
          p => new Row(Array(p.firstName, new Summable(p.age))),
          (a: Row, b: Row) => a.aggregate(b, Array(1))
        )
      }.map(_.normalize).toList.sortBy(_.data(0).toString)
    )
  }

  def supportGroupbyHaving = {
    executeAndMatch(
      select('firstName, sum('age) as 'ageSum) from table groupBy ('firstName) having ('ageSum > 10), {
        val linkedHash = new mutable.LinkedHashMap[Seq[Any], Row]
        Utils.groupBy[Person, Row](
          data,
          _.firstName,
          p => new Row(Array(p.firstName, new Summable(p.age))),
          (a: Row, b: Row) => a.aggregate(b, Array(1))
        )
      }.map(_.normalize).filter(_.data(1).asInstanceOf[Int] > 10).toList
    )
  }

  def supportSelectOrdering = {
    executeAndMatch(
      select(*) from table orderBy ('firstName),
      data.sortBy(_.firstName).map(
        p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.spouseId))
      )
    )
  }

  def supportSelectOrderingDesc = {
    executeAndMatch(
      select(*) from table orderBy ('firstName desc),
      data.sortBy(_.firstName)(Ordering.String.reverse).map(
        p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.spouseId))
      )
    )
  }

  def supportSelectLimitOffset = {
    executeAndMatch(
      select(*) from table limit (1, 3),
      data.slice(1, 4).map(
        p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.spouseId))
      )
    )
  }

  def supportSelectLimit = {
    executeAndMatch(
      select(*) from table limit (3),
      data.slice(0, 3).map(
        p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.spouseId))
      )
    )
  }

  /**************/
  /** UDF test **/
  /**************/
  def supportSelectCount = {
    executeAndMatch(
      select(count(*) as 'myCount) from table,
      List(new Row(Array(table.collectAsList().length)))
    )
  }

  def supportSelectCountWithGroupby = {
    executeAndMatch(
      select('firstName, count('age) as 'ageSum) from table groupBy ('firstName) having ('ageSum > 10), {
        val linkedHash = new mutable.LinkedHashMap[Seq[Any], Row]
        Utils.groupBy[Person, Row](
          data,
          _.firstName,
          p => new Row(Array(p.firstName, new Countable(1))),
          (a: Row, b: Row) => a.aggregate(b, Array(1))
        )
      }.map(_.normalize).filter(_.data(1).asInstanceOf[Int] > 10).toList
    )
  }

  def supportSelectDistinct = {
    executeAndMatch(
      selectDistinct('firstName) from table orderBy ('firstName),
      data.map(_.firstName).distinct.sorted.map(d => new Row(Array(d)))
    )
  }

  def supportSelectCountDistinct = {
    executeAndMatch(
      select(countDistinct('firstName)) from table,
      List(new Row(Array(data.map(_.firstName).distinct.size)))
    )
  }

  def supportSelectSubquery = {
    executeAndMatch(
      select(select(1)) from table,
      data.map(p => new Row(Array(1))).toList
    )
  }

  def supportFromSubquery = {
    executeAndMatch(
      select(*) from (select(*) from table),
      data.map(p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.spouseId))).toList
    )
  }

  def supportDetectBadSubquery = {
    try {
      executeAndMatch(
        select(select(1, 2)) from table,
        data.map(p => new Row(Array(1))).toList
      )
      throw new Exception()
    } catch {
      case ie: IllegalArgumentException =>
        assert(true)
      case e => throw e
    }
  }

}
