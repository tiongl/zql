package zql

import zql.core._

/**
 * Created by tiong on 12/5/16.
 */
package object spark {

  class PartitionStatement(states: Map[String, Any]) extends Statement(states) {
    val PARTITIONBY = "PARTITION_BY"
    def partitionBy(column: Seq[Column]) = newStatement(PARTITIONBY, column)

    def partitionBy = states(PARTITIONBY).asInstanceOf[Seq[Column]]
  }

  class Partitionable(val statement: Statement) extends StatementWrapper {
    class Partitioned(val statement: Statement) extends Limitable
    def partitionBy(columns: Column*) = new Partitioned(new PartitionStatement(statement.states).partitionBy(columns))
  }

  implicit def toPartitionStatement(limit: Limitable): Partitionable = {
    new Partitionable(limit.statement())
  }
}
