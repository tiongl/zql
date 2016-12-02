package zql.core

import zql.list.ListTable

class FuncSchemaTest extends TableTest {
  val table = ListTable.create[Person](
    ('id, _.id),
    ('firstName, _.firstName),
    ('lastName, _.lastName),
    ('age, _.age)
  )(data)
}
