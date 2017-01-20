package zql.core

import zql.sql.SqlGenerator

sealed class JoinType(val name: String)
object JoinType {
  val leftJoin = new JoinType("left")
  val rightJoin = new JoinType("right")
  val fullJoin = new JoinType("full")
  val innerJoin = new JoinType("inner")
}

abstract class JoinedTable(val tb1: Table, val tb2: Table, val joinType: JoinType) extends Table {
  var jointPoint: Condition = null

  def on(cond: Condition): JoinedTable = {
    if (jointPoint == null) {
      jointPoint = cond
    } else {
      throw new IllegalStateException("Join-point has been set")
    }
    this
  }

  override def toSql(gen: SqlGenerator): String = {
    val sb = new StringBuilder
    val t1 = tb1.toSql(gen)
    val t2 = tb2.toSql(gen)
    sb.append(t1 + " JOIN " + t2)
    if (jointPoint != null) {
      sb.append(" ON ")
      sb.append(gen.visit(jointPoint, null))
    }
    return sb.toString
  }

  override def as(alias: Symbol) = throw new IllegalArgumentException("Not supported as on joined table")

  override def join(table: Table): JoinedTable =
    throw new IllegalArgumentException("Not supported join as on joined table")

  override def innerJoin(table: Table): JoinedTable =
    throw new IllegalArgumentException("Not supported join as on joined table")

  override def leftJoin(table: Table): JoinedTable =
    throw new IllegalArgumentException("Not supported join as on joined table")

  override def rightJoin(table: Table): JoinedTable =
    throw new IllegalArgumentException("Not supported join as on joined table")

  override def fullJoin(table: Table): JoinedTable =
    throw new IllegalArgumentException("Not supported join as on joined table")

  protected def joinWithType(table: Table, jt: JoinType): JoinedTable =
    throw new IllegalArgumentException("Not supported join as on joined table")
}
