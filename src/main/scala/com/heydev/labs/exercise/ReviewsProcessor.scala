package com.heydev.labs.exercise

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}

object ReviewsProcessor {
  val REVIEW_SCHEMA: StructType = new StructType()
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

  def processReviews(df: DataFrame): DataFrame =
    df.select(from_json(df("value"), REVIEW_SCHEMA) as "review")
}
