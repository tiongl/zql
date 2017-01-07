package zql.core

import zql.list.ListTable

class FuncSchemaTest extends ListTableTest {

  override def personSchema = new ReflectedSchema[Person]("person") {
    i func('id, _.id)
    i func('firstName, _.firstName)
    i func('lastName, _.lastName)
    i func('age, _.age)
    i func('departmentId, _.departmentId)
  }

  override def departmentSchema = new ReflectedSchema[Department]("department") {
    i func('id, _.id)
    i func('name, _.name)
  }

}
