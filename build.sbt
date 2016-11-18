name := "zql"

version := "1.0"

scalaVersion := "2.11.8"

lazy val root =
  project.in( file(".") )
    .aggregate(core, spark)

lazy val core = project.in( file("zql-core") )

lazy val spark = project in file("zql-spark") dependsOn( core )