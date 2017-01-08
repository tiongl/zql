package zql.spark

import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.Outcome
import zql.core.{ Person, TableTest }
import zql.core._
import zql.rowbased.Row

//NOTE: If you have 'bad symbolic issue' issue regarding TableTest and Assertion in intellij,
// you need to export scalatest jar from core project using intellij module settings
class RDDTableTest extends TableTest {

  val config = new SparkConf().setAppName("SparkJoins").setMaster("local[4]")

  val sc = new SparkContext(config)

  val personRdd = sc.parallelize(persons)

  val departmentRdd = sc.parallelize(departments)

  val personTable = new RDDTable[Person](personSchema, personRdd)

  val departmentTable = new RDDTable[Department](departmentSchema, departmentRdd)

  it should "support partition by" in supportPartitionBy

  def supportPartitionBy = {
    executeAndMatch(
      select('firstName, 'lastName) from personTable partitionBy ('firstName), //we don't actually support partition by yet
      persons.map(p => new Row(Array(p.firstName, p.lastName)))
    )
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }

}
