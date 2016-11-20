package zql.core

import org.scalatest.{Matchers, FlatSpec}
import zql.core._

class ListTableTest extends FlatSpec with Matchers with PersonExample{

  def executeAndMatch(statement: Compilable, rows: List[Row]) = {
    val results = statement.compile.execute().collectAsList()
    results should be (rows)
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
      table select ('firstName, 'lastName, SUM('age)),
      List(new Row(Array(data(0).firstName, data(0).lastName, data.map(_.age).sum)))
    )
  }

//  it should "support simple filtering" in {
//    executeAndMatch(
//      table select ('firstName, 'lastName) where ('firstName==="test")
//    )
//  }
}
