#ZQL#
-----

##Description##

ZQL is a scala-based DSL that allows developer to quickly develop big-data application using uniform SQL-like interface. The main reasons for the development of ZQL are

1. To make application development faster by using SQL-like interface
2. To make the application less error-prone logically as business logic are  more readily translated to SQL query than low-level programming API of aggregation system
3. To make such application portable across most distributed aggregation system (like Spark, MR, or Apache Flink)

##Usage##
ZQL are distributed into two forms:

1. ZQL core api - which includes the language core
2. ZQL runtime - which includes runtime for specific aggregation system. For example, the runtime for Apache Spark and Apache Flink are separated as different runtime library so the developer only need to include the runtime they are targeting the application for. 

The following shows how one include the libray in sbt build file
```scala
require 'redcarpet'
markdown = Redcarpet.new("Hello World!")
puts markdown.to_html
```
3. 
Using ZQL
- General usage
- Extending Schema
- Implement UDF
- Customize option
Development
  - Design
    - Internal
  - Adding another runtime support
    - implement table
     - rowbased
     - passthrough
     - custom 

FAQ
1. Why scala DSL?
2. Will full SQL string expression supported
3. 
