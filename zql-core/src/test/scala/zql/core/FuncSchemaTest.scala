package zql.core

import zql.list.ListTable

class FuncSchemaTest extends TableTest {
  import FuncSchema._

  val schema = FuncSchema.create[Person](
    ('id, _.id),
    ('firstName,  _.firstName),
    ('lastName, _.lastName),
    ('age, _.age)
  )

  val table = new ListTable[Person](data, schema)

  it should "support derived column" in {
    val schema = FuncSchema.create[Person](
      ('id, _.id),
      ('firstName,  _.firstName),
      ('lastName, _.lastName),
      ('age, _.age),
      ('halfage, _.age/2) 
    )
    val table = new ListTable[Person](data, schema)
    executeAndMatch(
      table select ('halfage),
      data.map(p => new Row(Array(p.age/2))).toList
    )
  }

}
