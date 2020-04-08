
scalaVersion := "2.12.11"

name := "de-hwe-final"
organization := "com.heydev.labs"
version := "0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.2.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.8.5"
