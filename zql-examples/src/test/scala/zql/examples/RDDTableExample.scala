package zql.examples

import org.apache.spark.{SparkConf, SparkContext}
import zql.core._
import zql.spark.RDDTable

object RDDTableExample {
  def main(args: Array[String]) {
    //the domain object

    //the data
    val data = Seq( //
      new Person(0, "John", "Smith", 20, 4), //
      new Person(1, "John", "Doe", 71, 5), //
      new Person(2, "John", "Johnson", 5, -1), //
      new Person(3, "Adam", "Smith", 10, -1), //
      new Person(4, "Ann", "Smith", 10, 0), //
      new Person(4, "Anna", "Doe", 10, 1) //
    ).toList

    val config = new SparkConf().setAppName("SparkJoins").setMaster("local[4]")

    val sc = new SparkContext(config)

    val rdd = sc.parallelize(data)

    val listTable = RDDTable[Person]('id, 'firstName, 'lastName, 'age)(rdd)

    val stmt = select(*) from listTable where ('firstName === "John") limit (5) //pick first 5 johns

    val results = stmt.compile.execute()

    results.collectAsList()

    val statement2 = select('firstName, 'age) from results //we can chain the result for next statement

    val resultTable2 = statement2.compile.execute()

    val statement3 = select(sum('age)) from resultTable2 //and chain it again

    val resultTable3 = statement3.compile.execute()

    println(resultTable3.collectAsList()) //but everything is not executed until here (due to laziness of spark)

  }
}
