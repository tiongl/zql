package zql.core

import org.scalatest.{Matchers, FlatSpec}
import zql.core._
import zql.core.util.Utils
import zql.list.{ListTable, ReflectedSchema}

import scala.collection.mutable

class ListTableTest extends TableTest {

  val schema = new ReflectedSchema[Person](Set('id, 'firstName, 'lastName, 'age))

  val table = new ListTable[Person](data, schema)

}
