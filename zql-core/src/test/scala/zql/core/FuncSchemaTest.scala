package zql.core

import zql.list.ListTable

class FuncSchemaTest extends TableTest {
  val table = ListTable.create[Person](
    ('id, _.id),
    ('firstName, _.firstName),
    ('lastName, _.lastName),
    ('age, _.age),
    ('spoudId, _.spouseId)

  )(data)

  val table2 = ListTable.create[Person](
    ('id, _.id),
    ('fullName, (p: Person) => p.firstName + p.lastName),
    ('age, _.age),
    ('spoudId, _.spouseId)

  )(data)
}
