package zql.core

import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import zql.core.util.Utils
import scala.collection.mutable

abstract class TableTest extends FlatSpec with Matchers with BeforeAndAfterAll with PersonExample {


  def personTable: Table

  def departmentTable: Table

  def personSchema = new ReflectedSchema[Person]("person"){
    o INT 'id
    o STRING 'firstName
    o STRING 'lastName
    o INT 'age
    o INT 'departmentId
  }

  def departmentSchema = new ReflectedSchema[Department]("department"){
    o INT 'id
    o STRING 'name
  }

  def executeAndMatch(statement: StatementWrapper, rows: List[Row]) = {
    val results = statement.compile.execute().collectAsList().map(r => normalizeRow(r))
    println("Query = " + statement.statement().toSql())
    println("Results = " + results)
    println("Results size = " + results.length)
    println("Expected = " + rows)
    println("Expected size = " + rows.size)
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

  //joined query
  it should "support cross product join" in supportCrossProductJoin
  it should "support (same table) join " in supportSameTableJoinTable
  it should "support (same table) join-on " in supportSameTableJoinOn
  it should "support (same table) join with filter (same table)" in supportSameTableJoinTableWithFilter
  it should "support join with original column name" in supportJoinTableWithOriginalColumnName
  it should "support join with filter (different table)" in supportJoinTableWithFilter

  //misc
  it should "support table alias" in supportTableAlias

  def supportAllOperations = {
    val one = new IntLiteral(1)
    val str = new StringLiteral("test")
    executeAndMatch(
      //TODO: string + is not working
      select(one + 1, one - 1, one * 2, one / 2) from personTable,
      persons.map(p => new Row(Array(1 + 1, 1 - 1, 1 * 2, 1 / 2.toFloat)))
    )

    //equality
    executeAndMatch(
      select(one > 1, one > 0, one > -1, one >= 1, one >= 0, one >= -1, one === 1, one === 0, one === -1, one !== 1, one !== 0, one !== -1, one < 1, one < 0, one < -1, one <= 1, one <= 0, one <= -1) from personTable,
      persons.map(p => new Row(Array(1 > 1, 1 > 0, 1 > -1, 1 >= 1, 1 >= 0, 1 >= -1, 1 == 1, 1 == 0, 1 == -1, 1 != 1, 1 != 0, 1 != -1, 1 < 1, 1 < 0, 1 < -1, 1 <= 1, 1 <= 0, 1 <= -1)))
    )
  }

  def supportSelectWithMathOperations = {
    executeAndMatch(
      select(1 + 1, 'firstName, 'age + 1) from personTable,
      persons.map(p => new Row(Array(2, p.firstName, p.age + 1))).toList
    )
  }

  def supportSelectLiteral = {
    executeAndMatch(
      select(1, "test", true) from personTable,
      persons.map(p => new Row(Array(1, "test", true))).toList
    )
  }

  def supportSelectAll = {
    executeAndMatch(
      select(*) from personTable,
      persons.map(p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.departmentId))).toList
    )
  }

  def supportSelectDataColumn = {
    executeAndMatch(
      select('firstName, 'lastName) from personTable,
      persons.map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  def supportDetectInvalidAggregation = {
    try {
      executeAndMatch(
        select('firstName, 'lastName, sum('age)) from personTable,
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
      select('firstName, 'lastName) from personTable where ('firstName === "John"),
      persons.filter(_.firstName == "John").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  def supportFilteringWithAggregation = {
    executeAndMatch(
      select(sum('age)) from personTable where ('firstName === "John"),
      List(new Row(Array(persons.filter(_.firstName == "John").map(_.age).sum)))
    )
  }

  def supportFilteringWithBooleanLogic = {
    executeAndMatch(
      select('firstName, 'lastName) from personTable where (('firstName === "John" and 'lastName === "Smith") or ('lastName === "Doe")),
      persons.filter(p => (p.firstName == "John" && p.lastName == "Smith") || p.lastName == "Doe").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  def supportNotFilter = {
    executeAndMatch(
      select('firstName, 'lastName) from personTable where (NOT('firstName === "John")),
      persons.filter(p => p.firstName != "John").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  def supportDetectInvalidAggregation2 = {
    try {
      val stmt = select('firstName, 'lastName) from personTable groupBy ('firstName)
      stmt.compile
      throw new Exception("Groupby without aggregate function must fail")

    } catch {
      case e: IllegalArgumentException =>
        e.getMessage should be("Group by must have at least one aggregation function")
    }
  }

  def supportSimpleGroupFunction = {
    executeAndMatch(
      select('firstName, sum('age)) from personTable groupBy ('firstName) orderBy ('firstName), {
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

  def supportGroupbyHaving = {
    executeAndMatch(
      select('firstName, sum('age) as 'ageSum) from personTable groupBy ('firstName) having ('ageSum > 10), {
        val linkedHash = new mutable.LinkedHashMap[Seq[Any], Row]
        Utils.groupBy[Person, Row](
          persons,
          _.firstName,
          p => new Row(Array(p.firstName, new Summable(p.age))),
          (a: Row, b: Row) => a.aggregate(b, Array(1))
        )
      }.map(_.normalize).filter(_.data(1).asInstanceOf[Int] > 10).toList
    )
  }

  def supportSelectOrdering = {
    executeAndMatch(
      select(*) from personTable orderBy ('firstName),
      persons.sortBy(_.firstName).map(
        p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.departmentId))
      )
    )
  }

  def supportSelectOrderingDesc = {
    executeAndMatch(
      select(*) from personTable orderBy ('firstName desc),
      persons.sortBy(_.firstName)(Ordering.String.reverse).map(
        p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.departmentId))
      )
    )
  }

  def supportSelectLimitOffset = {
    executeAndMatch(
      select(*) from personTable limit (1, 3),
      persons.slice(1, 4).map(
        p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.departmentId))
      )
    )
  }

  def supportSelectLimit = {
    executeAndMatch(
      select(*) from personTable limit (3),
      persons.slice(0, 3).map(
        p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.departmentId))
      )
    )
  }

  /**************/
  /** UDF test **/
  /**************/
  def supportSelectCount = {
    executeAndMatch(
      select(count(*) as 'myCount) from personTable,
      List(new Row(Array(personTable.collectAsList().length)))
    )
  }

  def supportSelectCountWithGroupby = {
    executeAndMatch(
      select('firstName, count('age) as 'ageSum) from personTable groupBy ('firstName) having ('ageSum > 10), {
        val linkedHash = new mutable.LinkedHashMap[Seq[Any], Row]
        Utils.groupBy[Person, Row](
          persons,
          _.firstName,
          p => new Row(Array(p.firstName, new Countable(1))),
          (a: Row, b: Row) => a.aggregate(b, Array(1))
        )
      }.map(_.normalize).filter(_.data(1).asInstanceOf[Int] > 10).toList
    )
  }

  def supportSelectDistinct = {
    executeAndMatch(
      selectDistinct('firstName) from personTable orderBy ('firstName),
      persons.map(_.firstName).distinct.sorted.map(d => new Row(Array(d)))
    )
  }

  def supportSelectCountDistinct = {
    executeAndMatch(
      select(countDistinct('firstName)) from personTable,
      List(new Row(Array(persons.map(_.firstName).distinct.size)))
    )
  }

  def supportSelectSubquery = {
    executeAndMatch(
      select(select(1)) from personTable,
      persons.map(p => new Row(Array(1))).toList
    )
  }

  def supportFromSubquery = {
    executeAndMatch(
      select(*) from (select(*) from personTable),
      persons.map(p => new Row(Array(p.id, p.firstName, p.lastName, p.age, p.departmentId))).toList
    )
  }

  def supportDetectBadSubquery = {
    try {
      executeAndMatch(
        select(select(1, 2)) from personTable,
        persons.map(p => new Row(Array(1))).toList
      )
      throw new Exception()
    } catch {
      case ie: IllegalArgumentException =>
        assert(true)
      case e => throw e
    }
  }

  def supportTableAlias = {
    executeAndMatch(
      select(c"t1.firstName", c"t1.lastName", c"t1.age") from (personTable as 't1),
      persons.map(p => new Row(Array(p.firstName, p.lastName, p.age))).toList
    )
  }

  def supportCrossProductJoin = {
    executeAndMatch(
      select(*) from (personTable as 't1, personTable as 't2) orderBy (c"t1.id", c"t2.id"),
      persons.flatMap {
        t1 =>
          persons.map {
            t2 =>
              val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t2.id, t2.firstName, t2.lastName, t2.age, t2.departmentId)
              new Row(all)
          }
      }
    )
  }

  def supportSameTableJoinTable = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (personTable as 't2)) orderBy (c"t1.id", c"t2.id"),
      persons.flatMap {
        t1 =>
          persons.map {
            t2 =>
              val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t2.id, t2.firstName, t2.lastName, t2.age, t2.departmentId)
              new Row(all)
          }
      }
    )
  }

  def supportSameTableJoinOn = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (personTable as 't2) on (c"t1.id" === c"t2.id")) orderBy (c"t1.id", c"t2.id"),
      persons.map {
        t1 =>
          val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId)
          new Row(all)
      }
    )
  }

  def supportSameTableJoinTableWithFilter = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (personTable as 't2) on (c"t1.id" === c"t2.id")) where c"t1.age" > 10 orderBy (c"t1.id", c"t2.id"),
      persons.filter(_.age > 10).map {
        t1 =>
          val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId)
          new Row(all)
      }
    )
  }

  def supportJoinTableWithFilter = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (departmentTable as 't2) on (c"t1.departmentId" === c"t2.id")) where c"t1.age" > 10 orderBy (c"t1.id"),
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

  def supportJoinTableWithOriginalColumnName = {
    executeAndMatch(
      select('firstName, 'lastName, 'name) from ((personTable) join (departmentTable) on (c"person.departmentId" === c"department.id")) where c"person.age" > 10 orderBy (c"firstName"),
      persons.filter(_.age > 10).sortBy(_.firstName).flatMap {
        t1 =>
          departments.flatMap {
            t2 =>
              if (t1.departmentId == t2.id) {
                val all = Array[Any](t1.firstName, t1.lastName, t2.name)
                Seq(new Row(all))
              } else {
                Seq()
              }
          }
      }
    )
  }
}
