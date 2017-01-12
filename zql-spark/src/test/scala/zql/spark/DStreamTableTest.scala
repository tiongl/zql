package zql.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.scalatest.{ BeforeAndAfterEach, Assertion }
import zql.core._
import zql.rowbased.Row

import scala.collection.mutable.Queue

class DStreamTableTest extends StreamingTableTest with BeforeAndAfterEach {

  val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[4]")
  // Create the context
  var ssc: StreamingContext = null

  var personTable: Table = null

  var departmentTable: Table = null

  override protected def beforeEach(): Unit = {
    println("Before")
    ssc = new StreamingContext(sparkConf, Seconds(1))
    val tmp = java.io.File.createTempFile("DStreamTableTest", "test")
    tmp.delete()
    tmp.mkdirs()
    tmp.deleteOnExit()
    ssc.checkpoint(tmp.getAbsolutePath)

    val personRdds = persons.map(p => ssc.sparkContext.makeRDD[Person](Seq(p)))
    val departmentRdds = departments.map(d => ssc.sparkContext.makeRDD[Department](Seq(d)))

    val personQueue = Queue(personRdds: _*)
    val departmentQueue = Queue[RDD[Department]](departmentRdds: _*)
    // Create the QueueInputDStream and use it do some processing
    val personStream = ssc.queueStream(personQueue)
    val departmentStream = ssc.queueStream(departmentQueue)
    personTable = DStreamTable[Person](personSchema, personStream)
    departmentTable = DStreamTable[Department](departmentSchema, departmentStream)
  }

  override def executeAndMatch(statement: StatementWrapper, rows: List[Row]) = {
    val resultTable = statement.compile.execute().asInstanceOf[DStreamTable[_]]
    val collector = resultTable.getSnapshotCollector()
    println("SQL = " + statement.statement().toSql())
    println("Starting spark context")
    ssc.start()
    Thread.sleep(10000)
    val results = collector.collect.asInstanceOf[List[Row]]
    println("Results = " + results.sorted.mkString(", "))
    println("Expected = " + rows.sorted.mkString(", "))
    assert(results.sorted.equals(rows.sorted))
    results.sorted should be(rows.sorted)
  }

  override protected def afterEach(): Unit = {
    println("After")
    println("Stopping spark context")
    ssc.stop()
    ssc = null
  }
}
