package zql.spark

import org.apache.spark.{SparkConf, SparkContext}
import zql.core.{ReflectedSchema, Person, TableTest}
import zql.list.ListTable

/**
  * Created by tiong on 11/24/16.
  */
class RDDTableTest extends TableTest{

  val schema = new ReflectedSchema[Person](Seq('id, 'firstName, 'lastName, 'age))

  val config = new SparkConf().setAppName("SparkJoins").setMaster("local")

  val sc = new SparkContext(config)

  val rdd = sc.parallelize(data)

  val table = new RDDTable[Person](rdd, schema)
}
