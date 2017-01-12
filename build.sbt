import sbt.Keys._

name := "zql"

version := "1.0.0-alpha1"

//scalaVersion := "2.11.8"

scalaVersion in ThisBuild := "2.11.8"

crossScalaVersions := Seq("2.11.8")

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused",
  "-Ytyper-debug"
)

lazy val root =
  project.in( file(".") )
    .aggregate(core, spark, flink)

lazy val core = project.in( file("zql-core") ) settings (
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5",
    libraryDependencies += "com.twitter" %% "util-eval" % "6.40.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0"
  )

lazy val spark = project in file("zql-spark") dependsOn( core % "compile->compile;test->compile;test->test") settings (
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
  libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
  parallelExecution in Test := false //to avoid multi sparkcontext in single jvm issue
  )

lazy val flink = project in file("zql-flink") dependsOn( core % "compile->compile;test->compile;test->test") settings (
  libraryDependencies += "org.apache.flink" % "flink-core" % "1.1.3" % "provided",
  libraryDependencies += "org.apache.flink" %% "flink-table" % "1.1.3" % "provided",
  parallelExecution in Test := false //to avoid multi sparkcontext in single jvm issue
  )

lazy val examples = project in file("zql-examples") dependsOn(
  core % "compile->compile;test->compile;test->test",
  spark % "compile->compile;test->compile;test->test") settings (
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
  )

