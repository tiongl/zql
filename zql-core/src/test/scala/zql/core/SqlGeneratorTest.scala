package zql.core

import org.scalatest.{ FlatSpec, Matchers }
import zql.list.ListTable
import zql.sql.DefaultSqlGenerator

class SqlGeneratorTest extends FlatSpec with Matchers with PersonExample {

  val table: Table = ListTable[Person]("person", 'id, 'firstName, 'lastName, 'age)(persons)

  val option = new CompileOption

  def generateAndMatch(statement: StatementWrapper, sql: String) = {
    //val results = statement.statement.toSql()
    val results = new DefaultSqlGenerator().generateSql(statement.statement())
    results should be(sql)
  }

  it should "support all operations" in {
    val one = new IntLiteral(1)
    val str = new StringLiteral("" + table.name)
    generateAndMatch(
      select(one + 1, one - 1, one * 2, one / 2) from table,
      "SELECT (1 + 1), (1 - 1), (1 * 2), (1 / 2) FROM " + table.name
    )

    //equality
    generateAndMatch(
      select(one > 1, one > 0, one > -1, one >= 1, one >= 0, one >= -1, one === 1, one === 0, one === -1, one !== 1, one !== 0, one !== -1, one < 1, one < 0, one < -1, one <= 1, one <= 0, one <= -1) from table,
      "SELECT 1 > 1, 1 > 0, 1 > -1, 1 >= 1, 1 >= 0, 1 >= -1, 1 == 1, 1 == 0, 1 == -1, 1 != 1, 1 != 0, 1 != -1, 1 < 1, 1 < 0, 1 < -1, 1 <= 1, 1 <= 0, 1 <= -1 FROM " + table.name
    )
  }

  it should "support select columns" in {
    generateAndMatch(
      select('firstName, 'age + 1) from table,
      "SELECT firstName, (age + 1) FROM " + table.name
    )
  }

  it should "support select literal" in {
    generateAndMatch(
      select(1, "test", true) from table,
      "SELECT 1, 'test', true FROM " + table.name
    )
  }

  it should "support select all" in {
    generateAndMatch(
      select(*) from table,
      "SELECT * FROM " + table.name
    )
  }

  it should "support simple select" in {
    generateAndMatch(
      select('firstName, sum('age) as 'ageSum) from table where ('fistName === "John" and 'firstName === "John" or 'firstName === "John") groupBy ('firstName) having ('ageSum > 50) orderBy ('firstName, 'lastName desc) limit (1, 10), // orderBy ('age),
      "SELECT firstName, SUM(age) AS ageSum FROM " + table.name + " WHERE ((fistName == 'John' AND firstName == 'John') OR firstName == 'John') GROUP BY firstName HAVING ageSum > 50 ORDER BY firstName, lastName DESC LIMIT 1, 10"
    )
  }

}
