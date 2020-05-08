package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef}
import com.databricks.labs.validation.utils.SparkSessionWrapper
import org.apache.spark.sql.functions._

object QuickTest extends App with SparkSessionWrapper {

  import spark.implicits._

  val testDF = Seq(
    (1, 2, 3),
    (4, 5, 6),
    (7, 8, 9)
  ).toDF("retail_price", "scan_price", "cost")

  Rule("Reasonable_sku_counts", count(col("sku")), Bounds(lower = 20.0, upper = 200.0))

  val minMaxPriceDefs = Array(
    MinMaxRuleDef("MinMax_Retail_Price_Minus_Scan_Price", col("retail_price")-col("scan_price"), Bounds(0.0, 29.99)),
    MinMaxRuleDef("MinMax_Scan_Price_Minus_Retail_Price", col("scan_price")-col("retail_price"), Bounds(0.0, 29.99))
  )

  val someRuleSet = RuleSet(testDF)
    .add(Rule("retail_pass", col("retail_price"), Bounds(lower = 1.0, upper = 7.0)))
    .add(Rule("retail_agg_pass_high", max(col("retail_price")), Bounds(lower = 0.0, upper = 7.1)))
    .add(Rule("retail_agg_pass_low", min(col("retail_price")), Bounds(lower = 0.0, upper = 7.0)))
    .add(Rule("retail_fail_low", col("retail_price"), Bounds(lower = 1.1, upper = 7.0)))
    .add(Rule("retail_fail_high", col("retail_price"), Bounds(lower = 0.0, upper = 6.9)))
    .add(Rule("retail_agg_fail_high", max(col("retail_price")), Bounds(lower = 0.0, upper = 6.9)))
    .add(Rule("retail_agg_fail_low", min(col("retail_price")), Bounds(lower = 1.1, upper = 7.0)))
    .addMinMaxRules(minMaxPriceDefs: _*)
  val (rulesReport, passed) = someRuleSet.validate()

  testDF.show(20, false)
  rulesReport.show(20, false)
}
