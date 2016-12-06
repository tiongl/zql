package zql.core

import zql.list.ListTable

class ListTableTest extends TableTest {
  val table = ListTable[Person]('id, 'firstName, 'lastName, 'age, 'spouseId)(data)
}
