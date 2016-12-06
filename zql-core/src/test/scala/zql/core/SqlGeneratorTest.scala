package zql.core

import org.scalatest.{ FlatSpec, Matchers }
import zql.list.ListTable

class SqlGeneratorTest extends FlatSpec with Matchers with PersonExample {

  def table: Table = ListTable[Person]('id, 'firstName, 'lastName, 'age)(data)

  val option = new CompileOption

  def generateAndMatch(statement: StatementWrapper, sql: String) = {
    val results = statement.statement.toSql("test")
    results should be(sql)
  }

  it should "support all operations" in {
    val one = new IntLiteral(1)
    val str = new StringLiteral("test")
    generateAndMatch(
      table select (one + 1, one - 1, one * 2, one / 2),
      "SELECT (1 + 1), (1 - 1), (1 * 2), (1 / 2) FROM test"
    )

    //equality
    generateAndMatch(
      table select (one > 1, one > 0, one > -1, one >= 1, one >= 0, one >= -1, one === 1, one === 0, one === -1, one !== 1, one !== 0, one !== -1, one < 1, one < 0, one < -1, one <= 1, one <= 0, one <= -1),
      "SELECT 1 > 1, 1 > 0, 1 > -1, 1 >= 1, 1 >= 0, 1 >= -1, 1 == 1, 1 == 0, 1 == -1, 1 != 1, 1 != 0, 1 != -1, 1 < 1, 1 < 0, 1 < -1, 1 <= 1, 1 <= 0, 1 <= -1 FROM test"
    )
  }

  it should "support select columns" in {
    generateAndMatch(
      table select ('firstName, 'age + 1),
      "SELECT firstName, (age + 1) FROM test"
    )
  }

  it should "support select literal" in {
    generateAndMatch(
      table select (1, "test", true),
      "SELECT 1, 'test', true FROM test"
    )
  }

  it should "support select all" in {
    generateAndMatch(
      table select (*),
      "SELECT * FROM test"
    )
  }

  it should "support simple select" in {
    generateAndMatch(
      table select ('firstName, sum('age) as 'ageSum) where ('fistName === "John" and 'firstName === "John" or 'firstName === "John") groupBy ('firstName) having ('ageSum > 50) orderBy ('firstName, 'lastName desc) limit (1, 10), // orderBy ('age),
      "SELECT firstName, SUM(age) as ageSum FROM test WHERE ((fistName == 'John' AND firstName == 'John') OR firstName == 'John') GROUP BY  firstName HAVING ageSum > 50 ORDER BY firstName, lastName DESC LIMIT  1, 10"
    )
  }

}
