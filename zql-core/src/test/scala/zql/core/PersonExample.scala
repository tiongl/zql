package zql.core

import zql.list.{ReflectedSchema, ListTable}

trait PersonExample {
  @SchemaDef(columns = Array("id", "firstName", "lastName", "age"))
  case class Person(id: Int, val firstName: String, val lastName: String, age: Int, spouseId: Int) {
    override def toString() = "Person(%s, %s, %d)".format(firstName, lastName, age)

    def toRow() = Seq(firstName, lastName, age)
  }

  val data = Seq(//
    new Person(0, "John", "Smith", 20, 4), //
    new Person(1, "John", "Doe", 71, 5), //
    new Person(2, "John", "Johnson", 5, -1), //
    new Person(3, "Adam", "Smith", 10, -1), //
    new Person(4, "Ann", "Smith", 10, 0), //
    new Person(4, "Anna", "Doe", 10, 1) //
  ).toList
  val schema = new ReflectedSchema[Person](Set('id, 'firstName, 'lastName, 'age))

  val table = new ListTable[Person](data, schema)
}
