package zql.spark

import org.apache.spark.{SparkConf, SparkContext}
import zql.core.Person
import zql.core.TableTest
import zql.list.{ListTable, ReflectedSchema}

/**
  * Created by tiong on 11/24/16.
  */
class RDDTableTest extends TableTest{

  val schema = new ReflectedSchema[Person](Set('id, 'firstName, 'lastName, 'age))

  val config = new SparkConf().setAppName("SparkJoins").setMaster("local")

  val sc = new SparkContext(config)

  val rdd = sc.parallelize(data)

  val table = new RDDTable[Person](rdd, schema)
}
