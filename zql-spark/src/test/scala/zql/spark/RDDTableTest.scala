package zql.spark

import org.apache.spark.{ SparkConf, SparkContext }
import zql.core.{ Person, TableTest }
import zql.core._

/**
 * Created by tiong on 11/24/16.
 */
class RDDTableTest extends TableTest {

  val config = new SparkConf().setAppName("SparkJoins").setMaster("local")

  val sc = new SparkContext(config)

  val rdd = sc.parallelize(data)

  val table = RDDTable[Person]('id, 'firstName, 'lastName, 'age)(rdd)
}
