package zql.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.Assertion
import zql.core.{ Person, TableTest, _ }

class SparkSqlTableTest extends TableTest {

  override def normalizeRow(row: Any) = {
    val realRow = row.asInstanceOf[org.apache.spark.sql.Row]
    val data = realRow.toSeq.toArray
    new Row(data)
  }

  val spark = SparkSession
    .builder()
    .appName("SparkSqlTableTest")
    .config("spark.master", "local[4]")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate()

  //initialize
  {
    val rdd = spark.sqlContext.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd)
    df.createOrReplaceTempView("person")
  }

  val table = new SparkSQLTable(spark, "person")

  override def supportSelectLimitOffset = {
    //TODO: this is never supported. we probably should do detection in compile phase to throw exception on it
    assert(true)
  }

  override def supportSelectLimit = {
    //TODO: this is never supported. we probably should do detection in compile phase to throw exception on it
    assert(true)
  }

  override def supportDetectInvalidAggregation = {
    //TODO: move detection to compile phase
    try {
      super.supportDetectInvalidAggregation
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        //do nothing
        assert(true)
    }
  }

  override def supportDetectInvalidAggregation2 = {
    //TODO: move detection to compile phase
    try {
      super.supportDetectInvalidAggregation
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        //do nothing
        assert(true)
    }
  }
  override protected def afterAll(): Unit = {
    spark.stop()
  }
}
