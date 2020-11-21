name := "FinalProject"

version := "0.1"

scalaVersion := "2.11.8"

organization := "ca.mcit.bigdata"

val hadoopVersion = "2.7.3"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",
).map( _% hadoopVersion)

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"

resolvers += "cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"




