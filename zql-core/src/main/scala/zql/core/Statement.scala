package zql.core

trait Executable[+T]{
  def execute(): T
}
trait Compilable {
  def compile(): Executable[Table]
}


trait StatementWrapper extends Compilable {
  def statement(): Statement

  def compile = statement.compile
}

class StatementEnd(val statement: Statement) extends StatementWrapper

trait Limitable extends StatementWrapper{
  def limit(offset: Int, limit: Int) = new StatementEnd(statement.limit((offset, limit)))
}

trait Orderable extends Limitable {
  def orderBy(columns: Column*) = statement.orderBy(columns)
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

class WherePart(val statement: Statement) extends Groupable with Orderable with Limitable

class Selected(selects: Seq[Column], table: Table) extends Groupable with Whereable {
  val statement = new Statement(table, selects)
}

//
//class Statement(val table: Table, val selects: Column*) extends WhereClause {
//  def compile() = table.compile(this)
//}

case class Statement(val _from: Table = null,
                     val _selects: Seq[Column] = null,
                     val _where: Condition = null,
                     val _groupBy: Seq[Column] = null,
                     val _orderBy: Seq[Column] = null,
                     val _limit: (Int, Int) = null,
                     val _having: Condition = null) //having
                    extends Compilable
{
  def select(selects: Seq[Column]) =
    new Statement(_from, selects, _where, _groupBy, _orderBy, _limit, _having)

  def from(from: Table) =
    new Statement(from, _selects, _where, _groupBy, _orderBy, _limit, _having)

  def where(where: Condition) =
    new Statement(_from, _selects, where, _groupBy, _orderBy, _limit, _having)

  def groupBy(groupBy: Seq[Column]) =
    new Statement(_from, _selects, _where, groupBy, _orderBy, _limit, _having)

  def orderBy(orderBy: Seq[Column]) =
    new Statement(_from, _selects, _where, _groupBy, orderBy, _limit, _having)

  def limit(limit: (Int, Int)) =
    new Statement(_from, _selects, _where, _groupBy, _orderBy, limit, _having)

  def having(having: Condition) =
    new Statement(_from, _selects, _where, _groupBy, _orderBy, _limit, having)

  def compile =  _from.compile(this)
}

