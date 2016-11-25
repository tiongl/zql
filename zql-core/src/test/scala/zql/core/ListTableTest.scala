package zql.core

import zql.list.ListTable

class ListTableTest extends TableTest {

  val schema = new ReflectedSchema[Person](Seq('id, 'firstName, 'lastName, 'age))

  val table = new ListTable[Person](data, schema)

}
