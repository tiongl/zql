package zql.core

import org.scalatest.{Matchers, FlatSpec}
import zql.core._
import zql.core.util.Utils

import scala.collection.mutable

class ListTableTest extends FlatSpec with Matchers with PersonExample{

  def executeAndMatch(statement: Compilable, rows: List[Row]) = {
    val results = statement.compile.execute().collectAsList()
    results should be (rows)
  }
  it should "support select with math operations" in {
    executeAndMatch(
      table select (1 + 1, 'firstName, 'age + 1),
      data.map(p => new Row(Array(2, p.firstName, p.age + 1))).toList
    )
  }

  it should "support select literal" in {
    executeAndMatch(
      table select (1, "test", true),
      data.map(p => new Row(Array(1, "test", true))).toList
    )
  }


  it should "support select all" in {
    executeAndMatch(
      table select (*),
      data.map(p => new Row(Array(p.id, p.firstName, p.lastName, p.age))).toList
    )
  }

  it should "support simple select" in {
    executeAndMatch(
      table select ('firstName, 'lastName),
      data.map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  it should "support simple select with aggregation" in {
    executeAndMatch(
      table select ('firstName, 'lastName, sum('age)),
      List(new Row(Array(data(0).firstName, data(0).lastName, data.map(_.age).sum)))
    )
  }

  it should "support simple filtering" in {
    executeAndMatch(
      table select ('firstName, 'lastName) where ('firstName==="John"),
      data.filter(_.firstName=="John").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  it should "support filtering with aggregation" in {
    executeAndMatch(
      table select (sum('age)) where ('firstName==="John"),
      List(new Row(Array(data.filter(_.firstName=="John").map(_.age).sum)))
    )
  }

  it should "support and filtering" in {
    executeAndMatch(
      table select ('firstName, 'lastName) where ('firstName==="John" and 'lastName==="Smith"),
      data.filter(p => p.firstName=="John" && p.lastName=="Smith").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  it should "support or filtering" in {
    executeAndMatch(
      table select ('firstName, 'lastName) where ('firstName==="John" or 'lastName==="Smith"),
      data.filter(p => p.firstName=="John" || p.lastName=="Smith").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  it should "support not filtering" in {
    executeAndMatch(
      table select ('firstName, 'lastName) where (NOT('firstName==="John")),
      data.filter(p => p.firstName!="John").map(p => new Row(Array(p.firstName, p.lastName))).toList
    )
  }

  it should "validate groupby must have aggregate function" in {
    try {
      val stmt = table select('firstName, 'lastName) groupBy ('firstName)
      stmt.compile
      throw new Exception("Groupby without aggregate function must fail")
    } catch {
      case e: IllegalArgumentException =>
        e.getMessage should be ("Group by must have at least one aggregation function")
      case _ =>
        throw new Exception("Groupby without aggregate function must fail")

    }
  }

  it should "support simple groupby function" in {
    executeAndMatch(
      table select('firstName, sum('age)) groupBy ('firstName), {
        val linkedHash = new mutable.LinkedHashMap[Seq[Any], Row]
        Utils.groupBy[Person, Row](data,
          _.firstName,
          p => new Row(Array(p.firstName, new Summable(p.age))),
          (a: Row, b: Row) => a.aggregate(b, Array(1))
        )
      }.map(_.normalize).toList
    )
  }

  it should "support groupby having " in {
    executeAndMatch(
      table select('firstName, sum('age) as 'ageSum) groupBy ('firstName) having ('ageSum > 10), {
        val linkedHash = new mutable.LinkedHashMap[Seq[Any], Row]
        Utils.groupBy[Person, Row](data,
          _.firstName,
          p => new Row(Array(p.firstName, new Summable(p.age))),
          (a: Row, b: Row) => a.aggregate(b, Array(1))
        )
      }.map(_.normalize).filter(_.data(1).asInstanceOf[Int]>10).toList
    )
  }

}
