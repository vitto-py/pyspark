ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "FirstScalaProject"
  )

//ADD VM > --add-exports java.base/sun.nio.ch=ALL-UNNAMED

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.4.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-catalyst
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "3.4.0" //% Test

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.32"
