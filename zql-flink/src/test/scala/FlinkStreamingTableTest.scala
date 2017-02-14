import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.scalatest._
import zql.core._
import zql.flink._
import zql.rowbased.{StreamingTable, Row}

import scala.concurrent.Future

object FlinkStreamingTableTest {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def createSource[T](list: List[T])  = new SourceFunction[T]{
    override def cancel(): Unit = {

    }

    override def run(ctx: SourceContext[T]): Unit = {
      for (elem <- list) {
        ctx.collect(elem)
        Thread.sleep(1000)
      }
    }
  }

  def generateSource[T](list: List[T])(out: Collector[T]) = {
    while (true) {
      println("Running")
      for (elem <- list) {
        println("Sending " + elem)
        out.collect(elem)
        Thread.sleep(1000)
      }
    }
  }

}
class FlinkStreamingTableTest extends StreamingTableTest with BeforeAndAfterEach{

  var personTable: Table = null

  var departmentTable: Table = null

  var env: StreamExecutionEnvironment = null

  var starter: Thread = null

  var jobResult: JobExecutionResult = null

  override def beforeEach() = {
    implicit val personTypeInfo = new GenericTypeInfo[Person](() => new Person(-1, "test", "test", -1, -1))
    implicit val departmentTypeInfo = new GenericTypeInfo[Department](() => new Department(-1, "test"))

    env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.disableSysoutLogging

    val personStream = env.addSource[Person](FlinkStreamingTableTest.createSource(persons))

    val departmentStream = env.addSource[Department](FlinkStreamingTableTest.createSource(departments))

    personTable = new FlinkStreamingTable(personSchema, new DataStreamData(personStream))

    departmentTable = new FlinkStreamingTable(departmentSchema, new DataStreamData(departmentStream))

  }

  override def executeAndMatch(statement: StatementWrapper, rows: List[Row], sort: Boolean = true) = {
    val resultTable = statement.compile.execute().asInstanceOf[StreamingTable[_]]
    val collector = resultTable.getSnapshotCollector().asInstanceOf[DataStreamSnapshotCollector[Row]]
    println("SQL = " + statement.statement().toSql())
    println("Expected = " + rows.sorted.mkString(", "))
    startStreaming()
    val results = collector.collect(jobResult) //this is the special case
    println("Results = " + results.sorted.mkString(", "))
    assert(results.sorted.equals(rows.sorted))
    results.sorted should be(rows.sorted)
  }

  override def startStreaming(): Unit = {
    starter = new Thread(new Runnable {
      def run() {
        jobResult = env.execute()
      }
    })

    starter.start()
    Thread.sleep(10000)
  }

  override def afterEach() = {
    starter.stop()
  }
}
