package zql.core

import zql.list.ListTable

class FuncSchemaTest extends TableTest {
  val personTable = ListTable.create[Person](
    "person",
    ('id, _.id),
    ('firstName, _.firstName),
    ('lastName, _.lastName),
    ('age, _.age),
    ('departmentId, _.departmentId)
  )(persons)

  val departmentTable = ListTable.create[Department](
    "department",
    ('id, _.id),
    ('name, _.name)
  )(departments)

  val table2 = ListTable.create[Person](
    "person",
    ('id, _.id),
    ('fullName, (p: Person) => p.firstName + p.lastName),
    ('age, _.age),
    ('departmentId, _.departmentId)

  )(persons)
}
