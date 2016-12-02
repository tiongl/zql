package zql.example

import zql.list.ListTable
import zql.core._

object ListTableExample {
  def main(args: Array[String]) {
    //the domain object
    case class Person(id: Int, val firstName: String, val lastName: String, age: Int, spouseId: Int)

    //the data
    val data = Seq( //
      new Person(0, "John", "Smith", 20, 4), //
      new Person(1, "John", "Doe", 71, 5), //
      new Person(2, "John", "Johnson", 5, -1), //
      new Person(3, "Adam", "Smith", 10, -1), //
      new Person(4, "Ann", "Smith", 10, 0), //
      new Person(4, "Anna", "Doe", 10, 1) //
    ).toList

    val listTable = ListTable[Person]('id, 'firstName, 'lastName, 'age)(data)

    /** Example 1 **/
    val statement1 = listTable select (*) where ('firstName === "John") limit (5) //pick first 5 johns

    val resultTable1 = statement1.compile.execute()

    /** Example 2 **/
    val statement2 = resultTable1 select ('firstName, 'age) //select the firstname, age column

    val resultTable2 = statement2.compile.execute()

    /** Example 3 **/
    val statement3 = resultTable2 select (sum('age)) //sum the age

    val resultTable3 = statement3.compile.execute()

    println(resultTable3.collectAsList())

  }
}
