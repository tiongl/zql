package zql.core

import zql.sql.SqlGenerator

abstract class JoinedTable(val tb1: Table, val tb2: Table) extends Table {
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

}
