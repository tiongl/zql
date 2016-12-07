package zql.core

import zql.sql.SqlGenerator

import scala.collection.mutable

trait Executable[+T] {
  def execute(): T
}

class CompileOption extends mutable.HashMap[String, String] {

}

trait Compiler[T <: Table] {
  def compile(stmt: Statement, option: CompileOption = new CompileOption): Executable[T]
}

trait Compilable {
  def compile(): Executable[Table]
}

trait StatementWrapper extends Compilable {
  def statement(): Statement

  def compile = statement.compile
}

class StatementEnd(val statement: Statement) extends StatementWrapper

trait Limitable extends StatementWrapper {
  def limit(count: Int): StatementEnd = limit(0, count)
  def limit(offset: Int, count: Int): StatementEnd = new StatementEnd(statement.limit((offset, count)))
}

trait Orderable extends Limitable {
  def orderBy(columns: OrderSpec*) = new Ordered(statement.orderBy(columns))
}

class Ordered(val statement: Statement) extends Limitable

trait Havingable extends Orderable {
  def having(condition: Condition): Haved = new Haved(this.statement.having(condition))
}

class Haved(val statement: Statement) extends Orderable

trait Groupable extends Orderable {
  def groupBy(groupBys: Column*): Grouped = new Grouped(this.statement.groupBy(groupBys))
}

class Grouped(val statement: Statement) extends Havingable with Orderable

trait Whereable extends StatementWrapper {
  def where(condition: Condition): Whered = new Whered(statement.where(condition))
}

class Whered(val statement: Statement) extends Groupable

class Selected(val statement: Statement) extends Groupable with Whereable

trait Selectable extends StatementWrapper {
  def select(selects: Column*) = new Selected(statement().select(selects))
  def selectDistinct(selects: Column*) = new Selected(statement().select(selects).distinct())
}

case class Statement(val states: Map[String, Any] = Map()) extends Compilable {

  //TODO: Create feature class for this
  val SELECT = "SELECT"
  val FROM = "FROM"
  val WHERE = "WHERE"
  val GROUPBY = "GROUPBY"
  val ORDERBY = "ORDERBY"
  val LIMIT = "LIMIT"
  val HAVING = "HAVING"
  val DISTINCT = "DISTINCT"

  def newStatement(key: String, value: Any): Statement = {
    if (!states.contains(key)) newStatement(states + (key -> value))
    else throw new IllegalArgumentException("Repeating " + key + " in statement")
  }

  def newStatement(states: Map[String, Any]) = {
    new Statement(states)
  }

  def get[T](key: String): T = states.get(key) match {
    case None => null.asInstanceOf[T]
    case Some(r) => r.asInstanceOf[T]
  }

  def select(columns: Seq[Column]) = newStatement(SELECT, columns)

  def select = get[Seq[Column]](SELECT)

  def from(from: Table) = newStatement(FROM, from)

  def from = get[Table](FROM)

  def where(where: Condition) = newStatement(WHERE, where)

  def where = get[Condition](WHERE)

  def groupBy(groupBy: Seq[Column]) = newStatement(GROUPBY, groupBy)

  def groupBy = get[Seq[Column]](GROUPBY)

  def orderBy(orderBy: Seq[OrderSpec]) = newStatement(ORDERBY, orderBy)

  def orderBy = get[Seq[OrderSpec]](ORDERBY)

  def limit(limit: (Int, Int)) = newStatement(LIMIT, limit)

  def limit = get[(Int, Int)](LIMIT)

  def having(having: Condition) = newStatement(HAVING, having)

  def having = get[Condition](HAVING)

  def distinct() = newStatement(DISTINCT, true)

  def isDistinct() = get[Boolean](DISTINCT)

  def compile = from.compile(this)

  def toSql(tableName: String) = new SqlGenerator().generate(this, tableName)
}

