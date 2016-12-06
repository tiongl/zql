package zql.sql

import org.slf4j.LoggerFactory
import zql.core._
import zql.list.ListTable

class SqlGenerator {
  val logger = LoggerFactory.getLogger(classOf[SqlGenerator])

  def asSelect(col: Column): String = {
    if (col.alias != null) {
      s"${col} as ${col.alias.name}"
    } else {
      col.toString
    }
  }

  def asOrder(col: OrderSpec): String = {
    if (!col.ascending) {
      col.toString + " DESC"
    } else {
      col.toString
    }
  }

  def generate(stmt: Statement, tableName: String): String = {
    val sql = new StringBuilder("SELECT ")
    sql.append(stmt.select.map(asSelect(_)).mkString(", "))
    sql.append(" FROM ")
    sql.append(tableName)
    if (stmt.where != null) {
      sql.append(" WHERE ")
      sql.append(stmt.where)
    }
    if (stmt.groupBy != null) {
      sql.append(" GROUP BY  ")
      sql.append(stmt.groupBy.mkString(", "))
      if (stmt.having != null) {
        sql.append(" HAVING ")
        sql.append(stmt.having)
      }
    }
    if (stmt.orderBy != null) {
      sql.append(" ORDER BY ")
      sql.append(stmt.orderBy.map(asOrder(_)).mkString(", "))
    }
    if (stmt.limit != null) {
      sql.append(" LIMIT  ")
      sql.append(stmt.limit._1)
      sql.append(", ")
      sql.append(stmt.limit._2)
    }

    logger.info("Generated SQL = " + sql)
    sql.toString()
  }

  def compileColumn(col: Column): String = {
    col match {
      case cond: Condition => compileCondition(cond)
      case tc: TypedColumn[_] => tc.toString
      case _ =>
        throw new IllegalArgumentException("Unknown column type " + col)
    }
  }

  def compileCondition(cond: Condition): String = {
    cond match {
      case bc: BinaryCondition =>
        val a = compileCondition(bc.a)
        val b = compileCondition(bc.b)
        return a.toString + " " + cond + " " + b.toString
      case ec: EqualityCondition =>
        val a = compileColumn(ec.a)
        val b = compileColumn(ec.b)
        return a + " " + cond + " " + b
      case nc: NotCondition =>
        val a = compileColumn(nc.a)
        return a + " " + cond
      case _ =>
        throw new IllegalArgumentException("Unknown condition " + cond.toString)
    }
  }

  def main(args: Array[String]) {
    case class Person(id: Int, val firstName: String, val lastName: String, age: Int, spouseId: Int)

    //the data
    val data = Seq( //
      new Person(0, "John", "Smith", 20, 4), //
      new Person(1, "John", "Doe", 71, 5), //
      new Person(2, "John", "Johnson", 5, -1), //
      new Person(3, "Adam", "Smith", 10, -1), //
      new Person(4, "Ann", "Smith", 10, 0), //
      new Person(4, "Anna", "Doe", 10, 1) //
    ).toList

    val table = ListTable[Person]('firstName, 'lastName)(data)
    //    table select ('firstName, 'lastName, 'age) where ('firstName==="John") groupBy ('lastName)
  }
}
