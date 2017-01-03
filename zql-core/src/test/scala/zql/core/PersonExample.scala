package zql.core

case class Person(id: Int, val firstName: String, val lastName: String, age: Int, departmentId: Int) {
  override def toString() = "Person(%s, %s, %d)".format(firstName, lastName, age)

  def toRow() = Seq(firstName, lastName, age)
}

case class Department(id: Int, name: String)

trait PersonExample {
  val persons = Seq( //
    new Person(0, "John", "Smith", 20, 1), //
    new Person(1, "John", "Doe", 71, 0), //
    new Person(2, "John", "Johnson", 5, 1), //
    new Person(3, "Adam", "Smith", 10, 0), //
    new Person(4, "Ann", "Smith", 10, 3), //
    new Person(5, "Anna", "Doe", 10, 2) //
  ).toList

  val departments = Seq(
    new Department(0, "Finance"),
    new Department(1, "Engineering"),
    new Department(2, "Sales")
  ).toList
}
