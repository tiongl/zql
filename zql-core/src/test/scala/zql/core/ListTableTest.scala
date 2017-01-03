package zql.core

import zql.list.ListTable

class ListTableTest extends TableTest {
  val personTable = ListTable[Person]("person", 'id, 'firstName, 'lastName, 'age, 'departmentId)(persons)
  val departmentTable = ListTable[Department]("department", 'id, 'name)(departments)
}
