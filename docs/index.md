#ZQL#
-----

##Description##

ZQL is a scala-based DSL that allows developer to quickly develop big-data application using uniform SQL-like interface. 

##Why##
The main reasons for the development of ZQL are

1. To make application development faster by using SQL-like interface
2. To make the application less error-prone logically as business logic are  more readily translated to SQL query than low-level programming API of aggregation system
3. To make such application portable across most distributed aggregation system (like Spark, MR, or Apache Flink)

##Usage##
ZQL are distributed into two forms:

1. ZQL core api - which includes the language core
2. ZQL runtime - which includes runtime for specific aggregation system. 

For example, the runtime for Apache Spark and Apache Flink are separated as different runtime library so the developer only need to include the runtime they are targeting the application for. 

A List-based runtime is included in ZQL core api to allow execution of SQL over in-memory List structure.

The following shows how one include the libray in sbt build file

```scala
	    libraryDependencies += "zql" % "zql-core" % "1.0-beta",
	    libraryDependencies += "zql" % "zql-spark" % "1.0-beta",

```

To start writing a query, you just have to follow 3 steps
1. First create a table oject that represent the underlying runtime. 
2. Then you can write sql statement that you want to execute to the table
3. Finally, just execute the query and collect your results

Here's an example show how this is done:

```scala
	//the domain object
    case class Person(id: Int, val firstName: String, val lastName: String, age: Int, spouseId: Int)

    //the data
    val data = Seq( //
      new Person(0, "John", "Smith", 20, 4), //
      new Person(1, "John", "Doe", 71, 5), //
      new Person(2, "John", "Johnson", 5, -1), //
      new Person(3, "Adam", "Smith", 10, -1), //
      new Person(4, "Ann", "Smith", 10, 0), //
      new Person(4, "Anna", "Doe", 10, 1) //
    ).toList

    val listTable = ListTable[Person]('id, 'firstName, 'lastName, 'age)(data) //## Step 1

    val stmt = select(*) from listTable where ('firstName === "John") limit (5) //## Step 2

    val resultTable = stmt.compile.execute() //## Step 3
    
    val resultData = resultTable.collectAsList() 

```

Note that the resultTable itself is a table, so you further chain you queries to do further processing, or do join statement for different results.

##Supported Syntax##
As there are many variation in SQL-dialect, we only selectively implement some of the features that is most relevant now. Here are the list of support SQL dsl you can use

<https://github.com/theel/zql/blob/master/zql-core/src/test/scala/zql/core/DslTest.scala>

##Supported Runtime##

These are the supported runtimes and some notes about them. Not all runtime support all features. 

Runtime       | Table support | Description        
------------- | --------------|------------
core			 | ListTable     | This support processing of in memory list, sql style
spark         | RDDTable      | This support aggregation of RDD[T]
              | SparkSQLTable | This support SparkSQL (mainly passthrough)

##Laziness##
A runtime can be lazy in the sense that the execution doesn't trigger until you need to results. For example, spark runtime are lazy in the sense that all execution only happen when you try to collect the results. You can chain more than 2 statements in your application but the execution will happen during result collection phase

```scala
    val rdd = ... // RDD[Person]

    val listTable = RDDTable[Person]('id, 'firstName, 'lastName, 'age)(rdd)

    val stmt = select(*) from listTable where ('firstName === "John") limit (5) //pick first 5 johns

    val results = stmt.compile.execute()

    val statement2 = select('firstName, 'age) from results //we can chain the result for next statement

    val resultTable2 = statement2.compile.execute()

    val statement3 = select(sum('age)) from resultTable2 //and chain it again

    val resultTable3 = statement3.compile.execute()

    println(resultTable3.collectAsList()) //but everything is not executed until here (due to laziness of spark)

```

##Performance##
Certain runtime is executed with ZQL processing (e.g. Spark RDDTable) and its is not as sophasticated as SparkSQL. For example, RDDTable and ListTable uses java reflection to get the attributes out of But there are certain optimization that can be done to easy through providing functions for the data access. Below is an example of such optimization

```scala
  val table = ListTable.create[Person](
    ('id, _.id), //this give the function to access the field (rather than using reflection)
    ('firstName, _.firstName),
    ('lastName, _.lastName),
    ('fullName, (p: Person) => p.firstName + p.lastName), //this create a derive column that operate on the data
    ('age, _.age),
    ('spoudId, _.spouseId)
  )(data)

```
As you can see, there's also way to create optimization for operation that would otherwise otherwise be slow through execution engine.

Runtime specific performance improvement will always be a work in progress.

##Development##

###Implement UDF####

###Adding another runtime support###

    - implement table
     - rowbased
     - passthrough
     - custom 
     - 
##Design##
    - Internal

##FAQ##
1. Why scala DSL?
2. What about other languages?
3. Will full SQL string expression supported
4. What other runtime are planned