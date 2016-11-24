# ZQL

ZQL is a scala SQL-like DSL that is designed to be executable across different data persistent and distributed aggregation system including RDBMS, MapReduce, Spark, Storm and more. 

The main reasons for the inception of ZQL are:

1. It makes application more portable by having a universal SQL programming interface 
2. It reduces the chance of logical error when translative a business rule (SQL) into low level api (rdd, dataframe and what not)
3. It allows rapid development using the universal programming interface and avoid learning curve of low level api


## Features
The features of ZQL include:

-   SQL-based DSL

-   Extensible DSL to cover more use-cases

-   Extensible execution layer that can be used to extend the coverage of persistent and aggregation system

## Below is the design for ZQL.
<kdb>
![Design](docs/design.png?raw=true "Title")
</kbd>

The main ZQL DSL are decoupled from the runtime that support the execution of the query expresssion. The runtime for different aggregation technology can be implemented and added easily. The separation allow the DSL to be truly aggregation technology agnostic.

** Currently only List and Spark runtime are implemented.
##Example

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

    val listTable = new ListTable(data, schema)

    /** Example 1 **/
    val statement1 = listTable select (*) where ('firstName === "John") limit (5) //pick first 5 johns

    val resultTable1 = statement1.compile.execute()

    /** Example 2 **/
    val statement2 = resultTable1 select ('firstName, 'age) //select the firstname, age column

    val resultTable2 = statement2.compile.execute()

    /** Example 3 **/
    val statement3 = resultTable2 select (sum('age)) //sum the age

    val resultTable3 = statement3.compile.execute()

    println(resultTable3.collectAsList())
```
 

# Supported Syntax
Currently the DSL support the common semantic in SQL

- Select ... where ... group by ... having ... order by ... limit ...

- UDF support include count, sum, 

# Getting started
TODO



