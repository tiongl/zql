package zql.spark

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import zql.core.ExecutionPlan._
import zql.core._
import zql.sql.SqlGenerator

class SparkSQLTable(val session: SparkSession, val schema: Schema) extends Table {

  override def collectAsList(): List[Any] = {
    val df = session.sql("select * from " + name)
    df.collect().toList
  }

  override def join(t: Table) = t match {
    case st: SparkSQLTable =>
      new JoinedSparkSqlTable(session, this, st)
    case _ =>
      throw new IllegalArgumentException("Cannot join with different table type")
  }

  override def compile(stmt: Statement): Executable[Table] = new SparkSQLCompiler(this, session).compile(stmt, schema)

  override def as(alias: Symbol): Table = new SparkSQLTable(session, schema.as(alias))
}

object SparkSQLTable {

  def apply(session: SparkSession, tableName: String) = {
    val schema = new SimpleSchema(tableName){
      session.table(tableName).schema.fields.map(field => addSimpleColDef(Symbol(field.name), field.dataType.getClass)).toSeq
    }
    new SparkSQLTable(session, schema)
  }
}

class JoinedSparkSqlTable(val session: SparkSession, tb1: SparkSQLTable, tb2: SparkSQLTable) extends JoinedTable(tb1, tb2) {
  override def schema: Schema = new JoinedSchema(tb1, tb2)

  override def name: String = s"joined_${tb1.name}_${tb2.name}"

  override def collectAsList(): List[Any] = ???

  override def compile(stmt: Statement): Executable[Table] = {
    new SparkSQLCompiler(this, session).compile(stmt, schema)
  }

  override def as(alias: Symbol) = throw new IllegalArgumentException("Not supported as on joined table")

  override def join(table: Table): JoinedTable = throw new IllegalArgumentException("Not supported join as on joined table")
}

class SparkSQLCompiler(table: Table, session: SparkSession) extends Compiler[SparkSQLTable] {
  val logger = LoggerFactory.getLogger(classOf[SparkSQLCompiler])

  override def compile(stmt: Statement, schema: Schema, option: CompileOption): Executable[SparkSQLTable] = {
    val execPlan = plan("Query") {
      first("Run spark sql") {
        val sqlString = stmt.toSql()
        logger.info("Running sql " + sqlString)
        val df = session.sql(sqlString)

        val newTableName = table.name + "_" + UUID.randomUUID().toString.replace("-", "")
        df.createOrReplaceTempView(newTableName)
        SparkSQLTable(session, newTableName)
      }
    }
    execPlan
  }
}