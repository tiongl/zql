package zql.core

import zql.list.ListTable
import zql.rowbased.Row
import zql.schema.{ Schema, SimpleSchema }
import zql.sql.{ SqlGenerator, DefaultSqlGenerator }

import scala.collection.mutable
import scala.reflect.ClassTag

trait Executable[+T] {
  def execute(): T
}

class CompileOption extends mutable.HashMap[String, Object]

trait StatementCompiler[T <: Table] {
  def compile(stmt: Statement, schema: Schema, option: CompileOption = new CompileOption): Executable[T]
}

trait Compilable {
  def compile(): Executable[Table]
}

trait StatementWrapper extends Compilable {
  def statement(): Statement
  def compile = statement.compile
}

trait Limitable extends StatementWrapper {
  class Limited(val statement: Statement) extends StatementWrapper
  def limit(count: Int): Limited = limit(0, count)
  def limit(offset: Int, count: Int): Limited = new Limited(statement.limit((offset, count)))
}

trait Orderable extends Limitable {
  class Ordered(val statement: Statement) extends Limitable
  def orderBy(columns: OrderSpec*) = new Ordered(statement.orderBy(columns))
}

trait Havingable extends Orderable {
  class Haved(val statement: Statement) extends Orderable
  def having(condition: Condition): Haved = new Haved(this.statement.having(condition))
}

trait Groupable extends Orderable {
  class Grouped(val statement: Statement) extends Havingable with Orderable
  def groupBy(groupBys: Column*): Grouped = new Grouped(this.statement.groupBy(groupBys))
}

trait Whereable extends Groupable {
  class Whered(val statement: Statement) extends Groupable
  def where(condition: Condition): Whered = new Whered(statement.where(condition))
}

trait Fromable extends StatementWrapper {
  class Fromed(val statement: Statement) extends Whereable
  def from(table: Table): Fromed = new Fromed(statement.from(table))
  def from(stmt: StatementWrapper): Fromed = from(new SubQuery(stmt))
}

class SubQuery(statement: StatementWrapper) extends Table {

  lazy val executed = statement.compile.execute()

  override def schema: Schema = executed.schema

  override def join(table: Table): JoinedTable = ???

  override def collectAsList[T: ClassTag](): List[T] = executed.collectAsList()

  override def collectAsRowList: List[Row] = executed.collectAsRowList

  override def compile(stmt: Statement): Executable[Table] = executed.compile(stmt)

  override def as(alias: Symbol): Table = ???

  override def toSql(gen: SqlGenerator): String = {
    "(" + statement.statement().toSql() + ")"
  }

  override def innerJoin(table: Table): JoinedTable = ???

  override def leftJoin(table: Table): JoinedTable = ???

  override def rightJoin(table: Table): JoinedTable = ???

  override def fullJoin(table: Table): JoinedTable = ???

  override protected def joinWithType(table: Table, jt: JoinType): JoinedTable = ???
}

class Selected(distinct: Boolean, cols: Column*) extends Fromable {
  val statement = new Statement(Map()).select(cols).distinct(distinct)
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

  def distinct(bool: Boolean) = newStatement(DISTINCT, bool)

  def isDistinct() = get[Boolean](DISTINCT)

  def compile = from match {
    case null =>
      val schema = new SimpleSchema("test")
      new ListTable[Any](schema, List(1)).compile(this) //just a dummy table
    case tb: Table =>
      tb.compile(this)
  }

  def toSql() = new DefaultSqlGenerator().generateSql(this)
}

