package zql.spark

import java.util.UUID

import org.apache.spark.sql.SparkSession
import zql.core.ExecutionPlan._
import zql.core._

class SparkSQLTable(val session: SparkSession, val tableName: String, val alias: String = null) extends Table {

  val name = tableName


  lazy val schema = new Schema() {
    val allColumns = session.table(tableName).schema.fieldNames.map(name => new SimpleColumnDef(Symbol(name))).toSeq
  }

  override def collectAsList(): List[Any] = {
    val df = session.sql("select * from " + tableName)
    df.collect().toList
  }

  override def join(t: Table) = new JoinedTable(this, t)

  override def compile(stmt: Statement): Executable[Table] = new SparkSQLCompiler(this).compile(stmt, schema)

  override def as(alias: Symbol): Table = new SparkSQLTable(session, tableName, alias.name)
}


class SparkSQLCompiler(table: SparkSQLTable) extends Compiler[SparkSQLTable] {
  override def compile(stmt: Statement, schema: Schema, option: CompileOption): Executable[SparkSQLTable] = {
    val execPlan = plan("Query") {
      first("Run spark sql"){
        val sqlString = stmt.toSql()
        val df = table.session.sql(sqlString)
        val newTableName = table.tableName + "_" + UUID.randomUUID().toString.replace("-", "")
        df.createOrReplaceTempView(newTableName)
        new SparkSQLTable(table.session, newTableName)
      }
    }
    execPlan
  }
}