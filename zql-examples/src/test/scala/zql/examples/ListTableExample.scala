package zql.examples

import zql.core._
import zql.list.ListTable

object ListTableExample {

  def main(args: Array[String]) {

    //the data
    val data = Seq( //
      new Person(0, "John", "Smith", 20, 4), //
      new Person(1, "John", "Doe", 71, 5), //
      new Person(2, "John", "Johnson", 5, -1), //
      new Person(3, "Adam", "Smith", 10, -1), //
      new Person(4, "Ann", "Smith", 10, 0), //
      new Person(4, "Anna", "Doe", 10, 1) //
    ).toList


    val personSchema = new ReflectedSchema[Person]("person"){
      o INT 'id
      o STRING 'firstName
      o STRING 'lastName
      o INT 'age
      o INT 'departmentId
    }

    val listTable = new ListTable[Person](personSchema, data)

    val stmt = select(*) from listTable where ('firstName === "John") limit (5) //pick first 5 johns

    val results = stmt.compile.execute()

    /** Example 2 **/
    val statement2 = select('firstName, 'age) from results //select the firstname, age column

    val resultTable2 = statement2.compile.execute()

    /** Example 3 **/
    val statement3 = select(sum('age)) from resultTable2 //sum the age

    val resultTable3 = statement3.compile.execute()

    println(resultTable3.collectAsList())

  }
}
