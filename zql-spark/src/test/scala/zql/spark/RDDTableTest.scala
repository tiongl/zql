package zql.spark

import org.apache.spark.{ SparkConf, SparkContext }
import zql.core.{ Person, TableTest }
import zql.core._

//NOTE: If you have 'bad symbolic issue' issue regarding TableTest and Assertion in intellij,
// you need to export scalatest jar from core project using intellij module settings
class RDDTableTest extends TableTest {

  val config = new SparkConf().setAppName("SparkJoins").setMaster("local[4]")

  val sc = new SparkContext(config)

  val rdd = sc.parallelize(data)

  val table = RDDTable[Person]('id, 'firstName, 'lastName, 'age, 'spouseId)(rdd)

  it should "support partition by" in supportPartitionBy

  def supportPartitionBy = {
    executeAndMatch(
      table select ('firstName, 'lastName) partitionBy('firstName), //we don't actually support partition by yet
      data.map(p  => new Row(Array(p.firstName, p.lastName)))
    )
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
