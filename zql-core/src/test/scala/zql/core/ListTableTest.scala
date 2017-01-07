package zql.core

import zql.list.ListTable

class ListTableTest extends TableTest {

  val personTable = new ListTable[Person](personSchema, persons)

  val departmentTable = new ListTable[Department](departmentSchema, departments)
}
