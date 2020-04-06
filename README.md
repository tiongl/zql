# ZQL

ZQL is a scala-based DSL that allows developer to quickly develop big-data processing application using uniform SQL-like interface. 
It is designed to be executable across different data persistent and distributed aggregation system including RDBMS, MapReduce, Spark, Storm and more. 

#Why
The main reasons for the inception of ZQL are:

1. It makes application more portable by having a universal SQL programming interface 
2. It reduces the chance of logical error when translative a business logic into an application (because SQL is closer to application logic than low level api)
3. It allows rapid development using the universal programming interface and avoid learning curve of low level api


## Features
The features of ZQL include:

-   Unified SQL-based DSL for batch or streaming data processing across data process framework

-   Chainable SQL

-   Stateful streaming sql extension with windowing support (work-in-progress)

## Example

```scala
    //the domain object
    case class Person(id: Int, val firstName: String, val lastName: String, age: Int, spouseId: Int)

    //the schema
    val schema = new ReflectedSchema[Person](Set('id, 'firstName, 'lastName, 'age))

    //the data
    val data = Seq(//
      new Person(0, "John", "Smith", 20, 4), //
      new Person(1, "John", "Doe", 71, 5), //
      new Person(2, "John", "Johnson", 5, -1), //
      new Person(3, "Adam", "Smith", 10, -1), //
      new Person(4, "Ann", "Smith", 10, 0), //
      new Person(4, "Anna", "Doe", 10, 1) //
    ).toList

    val personTable = new ListTable(data, schema)

    /** Example 1 **/
    val statement1 = select (*) from personTable where ('firstName === "John") limit (5) //pick first 5 johns

    val resultTable1 = statement1.compile.execute()

    /** Example 2 **/
    val statement2 = select ('firstName, 'age) from resultTable1 //select the firstname, age column

    val resultTable2 = statement2.compile.execute()

    /** Example 3 **/
    val statement3 = select (sum('age)) from resultTable2 //sum the age

    val resultTable3 = statement3.compile.execute()

    println(resultTable3.collectAsList())
```
 


## Below is the design for ZQL.
<kdb>
![Design]
(https://github.com/theel/zql/blob/master/docs/design.png?raw=true)
</kbd>

The main ZQL DSL are decoupled from the runtime that support the execution of the query expresssion. The runtime for different aggregation technology can be implemented and added easily. The separation allow the DSL to be truly aggregation technology agnostic.

** Currently only List (ListTable) and Spark (RDDTable and SparkSQLTable) runtime are implemented. More will come soon.

##Supported Syntax##
As there are many variation in SQL-dialect, we only selectively implement some of the features that is most relevant now. 
Currently the DSL support the common semantic in SQL

Select ... where ... group by ... having ... order by ... limit ...
Join support (for tables of same runtime)
UDF support include count, sum

Here are the list of support SQL dsl you can use

<https://github.com/theel/zql/blob/master/zql-core/src/test/scala/zql/core/DslTest.scala>



## Supported Runtime ##

These are the supported runtimes and some notes about them. Not all runtime support all features. 

Runtime       | Table support   | Batch/Streaming |Description        
------------- | ----------------|-----------------|-----------
core          | ListTable       | Batch           |This supports processing of in memory list, sql style
spark         | RDDTable        | Batch           |This supports aggregation of RDD[T]
spark         | SparkSQLTable   | Batch           |This supports SparkSQL (mainly passthrough)
spark         | DStreamTable    | Streaming       |This supports spark streaming
flink         | FlinkDsTable    | Batch           |This supports flink dataset processing
flink         | FlinkStreamTable| Streaming       |This supports flink datastream processing
              
                            

# Getting started

As ZQL is a Scala-DSL, we assume you will be using it with Scala. Simply add the following to Scala build.sbt

```scala
	libraryDependencies += "zql" %% "zql-core" % "1.0.0-alpha1" % "provided",
	libraryDependencies += "zql" %% "zql-spark" % "1.0.0-alpha1" % "provided", //for spark runtime
```



