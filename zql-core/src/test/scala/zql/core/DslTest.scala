package zql.core

import org.scalatest._
import zql.list.{ReflectedSchema, ListTable}

class DslTest extends FlatSpec with Matchers with PersonExample{

  val schema = new ReflectedSchema[Person](Set('id, 'firstName, 'lastName, 'age))

  val table = new ListTable[Person](data, schema)

  "Here are all the supported syntax for the dsl" should "just compile" in {

    //all the possible selects
    table select (*)
    table select ('firstName, 'lastName, "Test") //simple select
    table select ('firstName, sum('age))         //select with UDF
    table select ('firstName) where ('firstName === 'lastName) //select with condition
    table select ('firstName) groupBy('firstName, 'lastName) //select with group by
    table select ('firstName) orderBy ('firstName) //select with order by
    table select ('firstName) limit(1, 10)

    //all the where
    val wherePart = table select('firstName) where ('firstName === 'lastName) //select with condition
    wherePart groupBy ('firstName)
    wherePart orderBy ('firstName)
    wherePart limit (1, 10)

    //all the groupby
    val groupPart = wherePart groupBy ('firstName)
    groupPart orderBy ('firstName)
    groupPart having ('firstName==='lastName)
    groupPart limit (1, 10)

    //orderby
    val orderByPart = table select('firstName) orderBy ('firstName) //select with order by
    orderByPart limit (1, 10)

    //all supported condition
    ('firstName === 'lastName) and ('firstName !== 'lastName)
    ('firstName === 'lastName) or NOT('firstName !== 'lastName)

    //functions
    NOT('age)
    sum('age)

  }
}