import sbt.Keys._

name := "zql"

version := "1.0"

scalaVersion := "2.11.8"

lazy val root =
  project.in( file(".") )
    .aggregate(core, spark)

lazy val core = project.in( file("zql-core") ) settings (
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  )

lazy val spark = project in file("zql-spark") dependsOn( core )
