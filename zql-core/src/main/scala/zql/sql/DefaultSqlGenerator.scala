package zql.sql

import zql.core._

class DefaultSqlGenerator extends SqlGenerator {

  def generateSql(stmt: Statement): String = {
    val builder = new StringBuilder
    builder.append("SELECT ")
    if (stmt.isDistinct()) {
      builder.append("DISTINCT ")
    }
    val selects = stmt.select.map(visit(_, null))
    builder.append(selects.mkString(", "))
    if (stmt.from != null) {
      val from = stmt.from
      builder.append(" FROM ")
      builder.append(stmt.from.toSql(this))
    }
    if (stmt.where != null) {
      builder.append(" WHERE ")
      builder.append(visit(stmt.where, null))
    }
    if (stmt.groupBy != null) {
      builder.append(" GROUP BY ")
      val groupBy = stmt.groupBy.map(visit(_, null))
      builder.append(groupBy.mkString(", "))
    }

    if (stmt.having != null) {
      builder.append(" HAVING ")
      builder.append(visit(stmt.having, null))
    }

    if (stmt.orderBy != null) {
      builder.append(" ORDER BY ")
      val orderBy = stmt.orderBy.map(visit(_, null))
      builder.append(orderBy.mkString(", "))
    }

    if (stmt.limit != null) {
      builder.append(" LIMIT ")
      builder.append(stmt.limit._1 + ", " + stmt.limit._2)
    }
    builder.toString()
  }

  override def handle(col: Column, context: String): String = {
    val builder = new StringBuilder
    builder.append(col.toSql(this)) //default to just call the col
    if (col.alias != null) {
      builder.append(s" AS ${col.alias.name}")
    }
    col match {
      case os: OrderSpec =>
        if (!os.ascending) {
          builder.append(" DESC")
        }
      case _ => //do nothing
    }
    builder.toString()
  }

}

