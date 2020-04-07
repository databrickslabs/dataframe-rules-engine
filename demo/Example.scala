package com.databricks.labs.validation

import com.databricks.labs.validation.utils.{Lookups, SparkSessionWrapper}
import com.databricks.labs.validation.utils.Structures._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Example extends App with SparkSessionWrapper {
  import spark.implicits._

  /**
   * Validation example
   * Passing pre-built array of rules into a RuleSet and validating a non-grouped dataframe
   */

  /**
   * Example of a proper UDF to simplify rules logic. Simplification UDFs should take in zero or many
   * columns and return one column
   * @param retailPrice column 1
   * @param scanPrice column 2
   * @return result column of applied logic
   */
  def getDiscountPercentage(retailPrice: Column, scanPrice: Column): Column = {
    (retailPrice - scanPrice) / retailPrice
  }

  // Example of creating array of custom rules
  val specializedRules = Array(
    Rule("Reasonable_sku_counts", count(col("sku")), Bounds(lower = 20.0, upper = 200.0)),
    Rule("Max_allowed_discount",
      max(getDiscountPercentage(col("retail_price"), col("scan_price"))),
      Bounds(upper = 90.0)),
    Rule("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 6.99)),
    Rule("Unique_Skus", countDistinct("sku"), Bounds(upper = 1.0))
  )

  // It's common to generate many min/max boundaries. These can be generated easily
  // The generator function can easily be extended or overridden to satisfy more complex requirements
  val minMaxPriceDefs = Array(
    MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
    MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99)),
    MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
  )

  val minMaxPriceRules = RuleSet.generateMinMaxRules(minMaxPriceDefs: _*)
  val someRuleSet = RuleSet(df)
  someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
  someRuleSet.addMinMaxRules("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 6.99))


  val catNumerics = Array(
    Rule("Valid_Stores", col("store_id"), Lookups.validStoreIDs),
    Rule("Valid_Skus", col("sku"), Lookups.validSkus)
  )

  val catStrings = Array(
    Rule("Valid_Regions", col("region"), Lookups.validRegions)
  )

  //TODO - validate datetime
  // Test, example data frame
  val df = sc.parallelize(Seq(
    ("Northwest", 1001, 123456, 9.32, 8.99, 4.23, "2020-02-01 00:00:00.000"),
    ("Northwest", 1001, 123256, 19.99, 16.49, 12.99, "2020-02-01"),
    ("Northwest", 1001, 123456, 0.99, 0.99, 0.10, "2020-02-01"),
    ("Northwest", 1001, 123456, 0.98, 0.90, 0.10, "2020-02-01"), // non_distinct sku
    ("Northwst", 1001, 123456, 0.99, 0.99, 0.10, "2020-02-01"), // Misspelled Region
    ("Northwest", 1002, 122987, 9.99, 9.49, 6.49, "2021-02-01"), // Invalid Date/Timestamp
    ("Northwest", 1002, 173544, 1.29, 0.99, 1.23, "2020-02-01"),
    ("Northwest", 1002, 168212, 3.29, 1.99, 1.23, "2020-02-01"),
    ("Northwest", 1002, 365423, 1.29, 0.99, 1.23, "2020-02-01"),
    ("Northwest", 1002, 3897615, 14.99, 129.99, 1.23, "2020-02-01"),
    ("Northwest", 1003, 163212, 3.29, 1.99, 1.23, "2020-02-01") // Invalid numeric store_id groupby test
  )).toDF("region", "store_id", "sku", "retail_price", "scan_price", "cost", "create_ts")
    .withColumn("create_ts", 'create_ts.cast("timestamp"))
    .withColumn("create_dt", 'create_ts.cast("date"))

  // Doing the validation
  // The validate method will return the rules report dataframe which breaks down which rules passed and which
  // rules failed and how/why. The second return value returns a boolean to determine whether or not all tests passed
//  val (rulesReport, passed) = RuleSet(df, Array("store_id"))
  val (rulesReport, passed) = RuleSet(df)
    .add(specializedRules)
    .add(minMaxPriceRules)
    .add(catNumerics)
    .add(catStrings)
    .validate(2)

  rulesReport.show(200, false)
//  rulesReport.printSchema()


}
