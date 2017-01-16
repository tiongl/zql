package zql.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.StreamingListener
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

  var personRdds: Seq[RDD[Person]] = null

  var departmentRdds: Seq[RDD[Department]] = null

  var personQueue: Queue[RDD[Person]] = null

  var departmentQueue: Queue[RDD[Department]] = null

  override protected def beforeEach(): Unit = {
    ssc = new StreamingContext(sparkConf, Seconds(1))
    val tmp = java.io.File.createTempFile("DStreamTableTest", "test")
    tmp.delete()
    tmp.mkdirs()
    tmp.deleteOnExit()
    ssc.checkpoint(tmp.getAbsolutePath)

    personRdds = persons.map(p => ssc.sparkContext.makeRDD[Person](Seq(p)))
    departmentRdds = departments.map(d => ssc.sparkContext.makeRDD[Department](Seq(d)))

    personQueue = new Queue[RDD[Person]]
    departmentQueue = new Queue[RDD[Department]]
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
    personRdds.foreach {
      p =>
        personQueue.enqueue(p)
        Thread.sleep(1000)
    }
    Thread.sleep(5000)
    val results = collector.collect.asInstanceOf[List[Row]]
    println("Results = " + results.sorted.mkString(", "))
    println("Expected = " + rows.sorted.mkString(", "))
    assert(results.sorted.equals(rows.sorted))
    results.sorted should be(rows.sorted)
  }

  override protected def afterEach(): Unit = {
    println("Stopping spark context")
    ssc.stop()
    ssc = null
  }
}
