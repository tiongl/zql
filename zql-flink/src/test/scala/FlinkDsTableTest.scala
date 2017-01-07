import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.TableEnvironment
import org.scalatest.Assertion
import zql.core._
import zql.flink.{FlinkDsTable, GenericTypeInfo, FlinkDsTable$}

/**
  * Created by tiong on 1/3/17.
  */
class FlinkDsTableTest extends TableTest {

  implicit val personTypeInfo = new GenericTypeInfo[Person](() => new Person(-1, "test", "test", -1, -1))
  implicit val departmentTypeInfo = new GenericTypeInfo[Department](() => new Department(-1, "test"))
  val env = ExecutionEnvironment.getExecutionEnvironment
  val personDs = env.fromElements(persons: _*)
  val departmentDs = env.fromElements(departments: _*)


  override def personTable: Table = FlinkDsTable[Person](personSchema, personDs)

  override def departmentTable: Table =  FlinkDsTable[Department](departmentSchema, departmentDs)

  override def supportSelectOrderingDesc: Assertion = assert(true)

  override def supportSelectLimitOffset: Assertion = assert(true)

  override def supportJoinTableWithOriginalColumnName: Assertion = assert(true)
}
