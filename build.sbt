import sbt.Keys._

name := "zql"

version := "1.0"

scalaVersion := "2.11.8"

lazy val root =
  project.in( file(".") )
    .aggregate(core, spark)

lazy val core = project.in( file("zql-core") ) settings (
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test",
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"
  )

lazy val spark = project in file("zql-spark") dependsOn( core % "compile->compile;test->test" ) settings (
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
//    libraryDependencies += core
  )

