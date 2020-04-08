package com.heydev.labs.exercise

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark Structured Streaming app
 *
 * Takes one argument, for Kafka bootstrap servers (ex: localhost:9092)
 */
object KafkaExample {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyStreamingApp"
  val schema: StructType = new StructType()
    .add("marketplace", StringType, nullable = true)
    .add("customer_id", IntegerType, nullable = true)
    .add("review_id", StringType, nullable = true)
    .add("product_id", StringType, nullable = true)
    .add("product_parent", IntegerType, nullable = true)
    .add("product_title", StringType, nullable = true)
    .add("product_category", StringType, nullable = true)
    .add("star_rating", IntegerType, nullable = true)
    .add("helpful_votes", IntegerType, nullable = true)
    .add("total_votes", IntegerType, nullable = true)
    .add("vine", StringType, nullable = true)
    .add("verified_purchase", StringType, nullable = true)
    .add("review_headline", StringType, nullable = true)
    .add("review_body", StringType, nullable = true)
    .add("review_date", TimestampType, nullable = true)

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "35.208.65.122:9092,34.68.16.1:9092,35.225.151.65:9092")
        .option("subscribe", "reviews")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")

      df.printSchema()

      val out = compute(df)

      val query = out.writeStream
        .outputMode(OutputMode.Complete())
        .format("console")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()

      query.processAllAvailable()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def compute(df: DataFrame): DataFrame = {
    df.select(from_json(df("value"), schema) as "js")
      .agg(round(avg("js.star_rating"), 2) as "avg_star_rating")
  }
}