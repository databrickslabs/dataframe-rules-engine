package com.databricks.labs.validation

import org.apache.spark.sql.SparkSession

trait SparkSessionFixture {
  lazy val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark session")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
}
