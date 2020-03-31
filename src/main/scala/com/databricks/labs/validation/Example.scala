package com.databricks.labs.validation

import com.databricks.labs.validation.utils.{SparkSessionWrapper, MinMaxFunc}
import com.databricks.labs.validation.utils.Structures._
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.col

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
   * @param scanPrince column 2
   * @return result column of applied logic
   */
  def getDiscountPercentage(retailPrice: Column, scanPrince: Column): Column = {
    ((retailPrice - scanPrince) / retailPrice).alias("total_discount_percent")
  }

  // Example of creating array of custom rules
  val specializedRules = Array(
    Rule("Reasonable_sku_counts", col("sku"), functions.count, "sku_count", Bounds(lower = 20.0, upper = 200.0)),
    Rule("Max_allowed_discount", getDiscountPercentage(col("retail_price"), col("scan_price")),
      functions.max, "total_discount", Bounds(upper = 90.0))
  )

  // It's common to generate many mins and max boundaries. These can be generated easily
  // The generator function can easily be extended or overridden to satisfy more complex requirements
  val minMaxPriceDefs = Array(
    MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 350.0)),
    MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 350.0)),
    MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 350.0))

  )
  val minMaxPriceRules = RuleSet.generateMinMaxRules(minMaxPriceDefs: _*)

  val x = Array(("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 350.0)))

  // Generate a single stand alone rule to demonstrate builder pattern below
  val standAloneRule = Rule("stddev_retail_price", col("retail_price"), functions.stddev, "stddev_price", Bounds(upper = 13.0))

  //TODO - validate datetime
  // TODO - validate distinct keys
  // Test, example data frame
  val df = sc.parallelize(Seq(
    (1001, 123456, 9.32, 8.99, 4.23),
    (1001, 123256, 19.99, 16.49, 12.99),
    (1001, 123456, 0.99, 0.99, 0.10),
    (1001, 123456, 0.98, 0.90, 0.10), // non_distinct sku
    (1002, 122987, 9.99, 9.49, 6.49),
    (1002, 173544, 1.29, 0.99, 1.23),
    (1002, 168212, 3.29, 1.99, 1.23),
    (1002, 365423, 1.29, 0.99, 1.23),
    (1002, 3897615, 14.99, 129.99, 1.23),
    (1003, 163212, 3.29, 1.99, 1.23)
  )).toDF("region", "store_id", "sku", "retail_price", "scan_price", "cost")

  // Doing the validation
  // The validate method will return the rules report dataframe which breaks down which rules passed and which
  // rules failed and how/why. The second return value returns a boolean to determine whether or not all tests passed
  val (rulesReport, passed) = RuleSet(df)
    .add(specializedRules)
    .add(standAloneRule)
    .add(minMaxPriceRules)
    .validate

  rulesReport.show(false)




}
