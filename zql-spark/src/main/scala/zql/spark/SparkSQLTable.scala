package zql.spark

import java.util.UUID

import org.apache.spark.sql.SparkSession
import zql.core._

class SparkSQLTable(val session: SparkSession, val tableName: String) extends Table {

  val name = tableName

  lazy val schema = new Schema() {
    val allColumns = session.table(tableName).schema.fieldNames.map(name => new SimpleColumnDef(Symbol(name))).toSeq
  }

  override def collectAsList(): List[Any] = {
    val df = session.sql("select * from " + tableName)
    df.collect().toList
  }

  override def compile(stmt: Statement): Executable[Table] = {
    val sqlString = stmt.toSql()
    val df = session.sql(sqlString)
    val newTableName = tableName + "_" + UUID.randomUUID().toString.replace("-", "")
    df.createOrReplaceTempView(newTableName)
    new Executable[Table] {
      override def execute(): Table = new SparkSQLTable(session, newTableName)
    }
  }
}
