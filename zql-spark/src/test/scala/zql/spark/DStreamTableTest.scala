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

  override def executeAndMatch(statement: StatementWrapper, rows: List[Row], sort: Boolean = true) = {
    val resultTable = statement.compile.execute().asInstanceOf[DStreamTable[_]]
    val collector = resultTable.getSnapshotCollector()
    println("SQL = " + statement.statement().toSql())
    println("Starting spark context")
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

  override def supportCrossProductJoin = assert(true) //can't really support it

  override def supportSameTableJoinTable = assert(true)

  override def supportSameTableJoinOn = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (personTable as 't2) on (c"t1.id" === c"t2.id")),
      persons.map {
        t1 =>
          val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId)
          new Row(all)
      }
    )
  }

  override def supportSameTableJoinTableWithFilter = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (personTable as 't2) on (c"t1.id" === c"t2.id")) where c"t1.age" > 10,
      persons.filter(_.age > 10).map {
        t1 =>
          val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId)
          new Row(all)
      }
    )
  }

  override def supportJoinTableWithFilter = {
    executeAndMatch(
      select(*) from ((personTable as 't1) join (departmentTable as 't2) on (c"t1.departmentId" === c"t2.id")) where c"t1.age" > 10,
      persons.filter(_.age > 10).flatMap {
        t1 =>
          departments.flatMap {
            t2 =>
              if (t1.departmentId == t2.id) {
                val all = Array(t1.id, t1.firstName, t1.lastName, t1.age, t1.departmentId, t2.id, t2.name)
                Seq(new Row(all))
              } else {
                Seq()
              }
          }
      }
    )
  }

}
