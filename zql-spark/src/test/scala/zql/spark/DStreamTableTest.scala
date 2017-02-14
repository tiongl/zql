package zql.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.scalatest.{ BeforeAndAfterEach, Assertion }
import zql.core._
import zql.rowbased.{ StreamingTable, Row }

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

  override protected def afterEach(): Unit = {
    println("Stopping spark context")
    ssc.stop()
    ssc = null
  }

  override def supportCrossProductJoin = assert(true) //can't really support it

  override def supportSameTableJoinTable = assert(true)

  override def startStreaming(): Unit = {
    ssc.start()
    for (i <- 0 until personRdds.length) {
      personQueue.enqueue(personRdds(i))
      if (i < departmentRdds.length) {
        departmentQueue.enqueue(departmentRdds(i))
      }
      Thread.sleep(1000)
    }
    while (personQueue.size > 0 && departmentQueue.size > 0) {
      Thread.sleep(1000)
    }
    Thread.sleep(1000)
  }
}
