package com.heydev.labs.exercise

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object Main {
  val logger: Logger = Logger.getLogger(this.getClass)

  val spark = SparkSession
    .builder()
    .appName("de-hwe-final")
    .master("local[1]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    spark.conf.set("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    spark.conf.set("spark.hadoop.fs.defaultFS", "hdfs://quickstart.cloudera:8020")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.208.65.122:9092,34.68.16.1:9092,35.225.151.65:9092")
      .option("subscribe", "reviews")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")

    val structuredReviews = ReviewsProcessor.processReviews(df)
    val newSchema = StructType(structuredReviews.schema.fields :+ UsersProcessor.USERS_SCHEMA)
    val encoder = RowEncoder(newSchema)
    val enrichedReviews = structuredReviews.mapPartitions (rows =>
      UsersProcessor.enrichUserData(rows, (row) => {
      row.getAs[Row]("review").getAs[Int]("customer_id").toString
    }))(encoder)

    val consoleQuery = enrichedReviews.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreach(new ForeachWriter[Row] {
        override def open(partitionId: Long, epochId: Long): Boolean = true

        override def process(value: Row): Unit = logger.info(value)

        override def close(errorOrNull: Throwable): Unit = { }
      })
      .start()

    val hdfsQuery = enrichedReviews.writeStream
      .format("parquet")
      .option("path", "hdfs://quickstart.cloudera:8020/user/brianrkeeter/reviews")
      .option("checkpointLocation", "hdfs://quickstart.cloudera:8020/user/brianrkeeter/reviews_checkpoint")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .start()

    consoleQuery.processAllAvailable()
//    hdfsQuery.processAllAvailable()
  }
}
