package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

case class ValidationValue(ruleName: String, passed: Boolean, permitted: String, actual: String)

class ValidatorTestSuite extends AnyFunSuite with SparkSessionFixture {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  test("The input dataframe should have no rule failures on MinMaxRule") {
    //  2 per rule so 2 MinMax_Sku_Price + 2 MinMax_Scan_Price + 2 MinMax_Cost + 2 MinMax_Cost_Generated
    // + 2 MinMax_Cost_manual = 10 rules
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")

    val expectedColumns = testDF.columns ++ Seq("MinMax_Sku_Price_min", "MinMax_Sku_Price_max", "MinMax_Scan_Price_min",
      "MinMax_Scan_Price_max", "MinMax_Cost_min", "MinMax_Cost_max", "MinMax_Cost_manual_min", "MinMax_Cost_manual_max",
      "MinMax_Cost_Generated_min", "MinMax_Cost_Generated_max")
    val expectedDF = Seq(
      (1, 2, 3,
        ValidationValue("MinMax_Sku_Price_min", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Sku_Price_max", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "2"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "2"),
        ValidationValue("MinMax_Cost_min", passed = true, "[0.0, 12.0]", "3"),
        ValidationValue("MinMax_Cost_max", passed = true, "[0.0, 12.0]", "3"),
        ValidationValue("MinMax_Cost_manual_min", passed = true, "[0.0, 12.0]", "3"),
        ValidationValue("MinMax_Cost_manual_max", passed = true, "[0.0, 12.0]", "3"),
        ValidationValue("MinMax_Cost_Generated_min", passed = true, "[0.0, 12.0]", "3"),
        ValidationValue("MinMax_Cost_Generated_max", passed = true, "[0.0, 12.0]", "3")
      ),
      (4, 5, 6,
        ValidationValue("MinMax_Sku_Price_min", passed = true, "[0.0, 29.99]", "4"),
        ValidationValue("MinMax_Sku_Price_max", passed = true, "[0.0, 29.99]", "4"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "5"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "5"),
        ValidationValue("MinMax_Cost_min", passed = true, "[0.0, 12.0]", "6"),
        ValidationValue("MinMax_Cost_max", passed = true, "[0.0, 12.0]", "6"),
        ValidationValue("MinMax_Cost_manual_min", passed = true, "[0.0, 12.0]", "6"),
        ValidationValue("MinMax_Cost_manual_max", passed = true, "[0.0, 12.0]", "6"),
        ValidationValue("MinMax_Cost_Generated_min", passed = true, "[0.0, 12.0]", "6"),
        ValidationValue("MinMax_Cost_Generated_max", passed = true, "[0.0, 12.0]", "6")
      ),
      (7, 8, 9,
        ValidationValue("MinMax_Sku_Price_min", passed = true, "[0.0, 29.99]", "7"),
        ValidationValue("MinMax_Sku_Price_max", passed = true, "[0.0, 29.99]", "7"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "8"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "8"),
        ValidationValue("MinMax_Cost_min", passed = true, "[0.0, 12.0]", "9"),
        ValidationValue("MinMax_Cost_max", passed = true, "[0.0, 12.0]", "9"),
        ValidationValue("MinMax_Cost_manual_min", passed = true, "[0.0, 12.0]", "9"),
        ValidationValue("MinMax_Cost_manual_max", passed = true, "[0.0, 12.0]", "9"),
        ValidationValue("MinMax_Cost_Generated_min", passed = true, "[0.0, 12.0]", "9"),
        ValidationValue("MinMax_Cost_Generated_max", passed = true, "[0.0, 12.0]", "9")
      )
    ).toDF(expectedColumns: _*)

    // Create an Array of MinMax Rules
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
    )

    // Generate the array of Rules from the minmax generator
    val rulesArray = RuleSet.generateMinMaxRules(MinMaxRuleDef("MinMax_Cost_Generated", col("cost"), Bounds(0.0, 12.0)))
    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)

    // Manually add a Rule
    someRuleSet.addMinMaxRules("MinMax_Cost_manual", col("cost"), Bounds(0.0, 12.0))
    someRuleSet.add(rulesArray)
    val validationResults = someRuleSet.validate()

    // Ensure that validate report is expected
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")

    // Ensure that there are 2 Rules per MinMax Rule added as separate columns
    assert(validationResults.completeReport.count() == 3)
    assert((validationResults.completeReport.columns diff testDF.columns).length == 10)

    // Ensure that all Rules passed;there should be no failed Rules
    assert(validationResults.summaryReport.count() == 0)
  }

  test("The input rule should have 3 invalid count for MinMax_Scan_Price_Minus_Retail_Price_min and max for failing complex type.") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")
    val expectedColumns = testDF.columns ++ Seq("MinMax_Retail_Price_Minus_Scan_Price_min", "MinMax_Retail_Price_Minus_Scan_Price_max",
      "MinMax_Scan_Price_Minus_Retail_Price_min", "MinMax_Scan_Price_Minus_Retail_Price_max")
    val expectedDF = Seq(
      (1, 2, 3,
        ValidationValue("MinMax_Retail_Price_Minus_Scan_Price_min", passed = false, "[0.0, 29.99]", "-1"),
        ValidationValue("MinMax_Retail_Price_Minus_Scan_Price_max", passed = false, "[0.0, 29.99]", "-1"),
        ValidationValue("MinMax_Scan_Price_Minus_Retail_Price_min", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Scan_Price_Minus_Retail_Price_max", passed = true, "[0.0, 29.99]", "1")
      ),
      (4, 5, 6,
        ValidationValue("MinMax_Retail_Price_Minus_Scan_Price_min", passed = false, "[0.0, 29.99]", "-1"),
        ValidationValue("MinMax_Retail_Price_Minus_Scan_Price_max", passed = false, "[0.0, 29.99]", "-1"),
        ValidationValue("MinMax_Scan_Price_Minus_Retail_Price_min", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Scan_Price_Minus_Retail_Price_max", passed = true, "[0.0, 29.99]", "1")
      ),
      (7, 8, 9,
        ValidationValue("MinMax_Retail_Price_Minus_Scan_Price_min", passed = false, "[0.0, 29.99]", "-1"),
        ValidationValue("MinMax_Retail_Price_Minus_Scan_Price_max", passed = false, "[0.0, 29.99]", "-1"),
        ValidationValue("MinMax_Scan_Price_Minus_Retail_Price_min", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Scan_Price_Minus_Retail_Price_max", passed = true, "[0.0, 29.99]", "1")
      )
    ).toDF(expectedColumns: _*)

    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Retail_Price_Minus_Scan_Price", col("retail_price") - col("scan_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price_Minus_Retail_Price", col("scan_price") - col("retail_price"), Bounds(0.0, 29.99))
    )

    // Generate the array of Rules from the minmax generator
    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val validationResults = someRuleSet.validate()

    // Ensure that validate report is expected
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")

    // Ensure that there are failed rows in summary report
    assert(validationResults.summaryReport.count() > 0)
    assert(validationResults.summaryReport.count() == 3)
  }

  test("The input rule should have 1 invalid count for failing aggregate type.") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")
    val expectedColumns = testDF.columns ++ Seq("MinMax_Min_Retail_Price", "MinMax_Min_Scan_Price")
    val expectedDF = Seq(
      (1, 2, 3,
        ValidationValue("MinMax_Min_Retail_Price", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Min_Scan_Price", passed = false, "[3.0, 29.99]", "2")
      ),
      (4, 5, 6,
        ValidationValue("MinMax_Min_Retail_Price", passed = true, "[0.0, 29.99]", "4"),
        ValidationValue("MinMax_Min_Scan_Price", passed = true, "[3.0, 29.99]", "5")
      ),
      (7, 8, 9,
        ValidationValue("MinMax_Min_Retail_Price", passed = true, "[0.0, 29.99]", "7"),
        ValidationValue("MinMax_Min_Scan_Price", passed = true, "[3.0, 29.99]", "8")
      )
    ).toDF(expectedColumns: _*)
    val minMaxPriceDefs = Seq(
      Rule("MinMax_Min_Retail_Price", min("retail_price"), Bounds(0.0, 29.99)),
      Rule("MinMax_Min_Scan_Price", min("scan_price"), Bounds(3.0, 29.99))
    )

    // Generate the array of Rules from the minmax generator
    val someRuleSet = RuleSet(testDF)
    someRuleSet.add(minMaxPriceDefs)
    val validationResults = someRuleSet.validate()

    // Ensure that validate report is expected
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")

    // Ensure that there is a failed row
    assert(validationResults.summaryReport.count() > 0)
    assert(validationResults.summaryReport.count() == 1)
  }

  test("The input dataframe should have exactly 1 rule failure on MinMaxRule") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 99)
    ).toDF("retail_price", "scan_price", "cost")
    val expectedColumns = testDF.columns ++ Seq("MinMax_Sku_Price_min", "MinMax_Sku_Price_max",
      "MinMax_Scan_Price_min", "MinMax_Scan_Price_max", "MinMax_Cost_min", "MinMax_Cost_max"
    )
    val expectedDF = Seq(
      (1, 2, 3,
        ValidationValue("MinMax_Sku_Price_min", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Sku_Price_max", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "2"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "2"),
        ValidationValue("MinMax_Cost_min", passed = true, "[0.0, 12.0]", "3"),
        ValidationValue("MinMax_Cost_max", passed = true, "[0.0, 12.0]", "3"),
      ),
      (4, 5, 6,
        ValidationValue("MinMax_Sku_Price_min", passed = true, "[0.0, 29.99]", "4"),
        ValidationValue("MinMax_Sku_Price_max", passed = true, "[0.0, 29.99]", "4"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "5"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "5"),
        ValidationValue("MinMax_Cost_min", passed = true, "[0.0, 12.0]", "6"),
        ValidationValue("MinMax_Cost_max", passed = true, "[0.0, 12.0]", "6"),
      ),
      (7, 8, 99,
        ValidationValue("MinMax_Sku_Price_min", passed = true, "[0.0, 29.99]", "7"),
        ValidationValue("MinMax_Sku_Price_max", passed = true, "[0.0, 29.99]", "7"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "8"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "8"),
        ValidationValue("MinMax_Cost_min", passed = false, "[0.0, 12.0]", "99"),
        ValidationValue("MinMax_Cost_max", passed = false, "[0.0, 12.0]", "99"),
      )
    ).toDF(expectedColumns: _*)

    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
    )

    // Generate the array of Rules from the minmax generator
    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val validationResults = someRuleSet.validate()

    // Ensure that validate report is expected
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")

    // Ensure that there is a failed row
    assert(validationResults.summaryReport.count() > 0)
    assert(validationResults.summaryReport.count() == 1)

    // Ensure that the the failed Rules are MinMax_Cost_min, MinMax_Cost_max
    assert(validationResults.summaryReport.select("failed_rules.ruleName").as[Array[String]].collect()(0)(0) == "MinMax_Cost_min", "MinMax_Cost_max")
    assert(validationResults.summaryReport.select("failed_rules.ruleName").as[Array[String]].collect()(0)(1) == "MinMax_Cost_max", "MinMax_Cost_max")
  }

  test("The DF in the rulesset object is the same as the input test df") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 99)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
    )
    // Generate the array of Rules from the minmax generator
    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val rulesDf = someRuleSet.getDf
    assert(testDF.except(rulesDf).count() == 0)
  }

  test("The group by columns are the correct group by clauses in the validation") {
    // 2 groups so count of the rules should yield (2 minmax rules * 2 columns) * 2 groups in cost (8 rows)
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 3)
    ).toDF("retail_price", "scan_price", "cost")
    val expectedColumns = Seq("cost", "MinMax_Sku_Price_min", "MinMax_Sku_Price_max", "MinMax_Scan_Price_min", "MinMax_Scan_Price_max")
    val expectedDF = Seq(
      (3,
        ValidationValue("MinMax_Sku_Price_min", passed = true, "[0.0, 29.99]", "1"),
        ValidationValue("MinMax_Sku_Price_max", passed = true, "[0.0, 29.99]", "7"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "2"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "8")
      ),
      (6,
        ValidationValue("MinMax_Sku_Price_min", passed = true, "[0.0, 29.99]", "4"),
        ValidationValue("MinMax_Sku_Price_max", passed = true, "[0.0, 29.99]", "4"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "5"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "5")
      )
    ).toDF(expectedColumns: _*)

    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99))
    )

    val someRuleSet = RuleSet(testDF, "cost")
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val groupBys = someRuleSet.getGroupBys
    val validationResults = someRuleSet.validate()

    // Ensure that input DF was grouped by "cost" column
    assert(groupBys.length == 1)
    assert(groupBys.head == "cost")
    assert(someRuleSet.isGrouped)

    // Ensure that all rows passed
    assert(validationResults.summaryReport.count() == 0)

    // Ensure that the complete report matches the expected output
    assert(validationResults.completeReport.count() == 2)
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
  }

  test("The group by columns are with rules failing the validation") {
    // 2 groups so count of the rules should yield (2 minmax rules * 2 columns) * 2 groups in cost (8 rows)
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 3)
    ).toDF("retail_price", "scan_price", "cost")
    val expectedColumns = Seq("cost", "MinMax_Sku_Price_min", "MinMax_Sku_Price_max", "MinMax_Scan_Price_min", "MinMax_Scan_Price_max")
    val expectedDF = Seq(
      (3,
        ValidationValue("MinMax_Sku_Price_min", passed = false, "[0.0, 0.0]", "1"),
        ValidationValue("MinMax_Sku_Price_max", passed = false, "[0.0, 0.0]", "7"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "2"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "8")
      ),
      (6,
        ValidationValue("MinMax_Sku_Price_min", passed = false, "[0.0, 0.0]", "4"),
        ValidationValue("MinMax_Sku_Price_max", passed = false, "[0.0, 0.0]", "4"),
        ValidationValue("MinMax_Scan_Price_min", passed = true, "[0.0, 29.99]", "5"),
        ValidationValue("MinMax_Scan_Price_max", passed = true, "[0.0, 29.99]", "5")
      )
    ).toDF(expectedColumns: _*)
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 0.0)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99))
    )

    val someRuleSet = RuleSet(testDF, "cost")
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val groupBys = someRuleSet.getGroupBys
    val validationResults = someRuleSet.validate()

    assert(groupBys.length == 1, "Group by length is not 1")
    assert(groupBys.head == "cost", "Group by column is not cost")
    assert(someRuleSet.isGrouped)

    // Ensure that there are failed rows
    assert(validationResults.summaryReport.count() > 0, "Rule set did not fail.")
    assert(validationResults.summaryReport.count() == 2, "Failed row count should be 2")
    assert(validationResults.completeReport.count() == 2, "Row count should be 2")

    // Ensure that the complete report matches expected output
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
  }

  test("Validate list of values with integer, double, and long types.") {
    val testDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L),
      ("food_b", 5.11, 6, 211111111111111L),
      ("food_c", 8.22, 99, 311111111111111L)
    ).toDF("product_name", "scan_price", "cost", "id")

    val expectedColumns = testDF.columns ++ Seq("CheckIfCostIsInLOV", "CheckIfScanPriceIsInLOV", "CheckIfIdIsInLOV")
    val numericLovExpectedDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L,
        ValidationValue("CheckIfCostIsInLOV", passed = true, "[3.0, 6.0, 99.0]", "3"),
        ValidationValue("CheckIfScanPriceIsInLOV", passed = true, "[2.51, 5.11, 8.22]", "2.51"),
        ValidationValue("CheckIfIdIsInLOV", passed = true, "[1.11111111111111E14, 2.11111111111111E14, 3.11111111111111E14]", "111111111111111")
      ),
      ("food_b", 5.11, 6, 211111111111111L,
        ValidationValue("CheckIfCostIsInLOV", passed = true, "[3.0, 6.0, 99.0]", "6"),
        ValidationValue("CheckIfScanPriceIsInLOV", passed = true, "[2.51, 5.11, 8.22]", "5.11"),
        ValidationValue("CheckIfIdIsInLOV", passed = true, "[1.11111111111111E14, 2.11111111111111E14, 3.11111111111111E14]", "211111111111111")
      ),
      ("food_c", 8.22, 99, 311111111111111L,
        ValidationValue("CheckIfCostIsInLOV", passed = true, "[3.0, 6.0, 99.0]", "99"),
        ValidationValue("CheckIfScanPriceIsInLOV", passed = true, "[2.51, 5.11, 8.22]", "8.22"),
        ValidationValue("CheckIfIdIsInLOV", passed = true, "[1.11111111111111E14, 2.11111111111111E14, 3.11111111111111E14]", "311111111111111")
      )
    ).toDF(expectedColumns: _*)

    val numericRules = Array(
      Rule("CheckIfCostIsInLOV", col("cost"), Array(3, 6, 99)),
      Rule("CheckIfScanPriceIsInLOV", col("scan_price"), Array(2.51, 5.11, 8.22)),
      Rule("CheckIfIdIsInLOV", col("id"), Array(111111111111111L, 211111111111111L, 311111111111111L))
    )

    // Generate the array of Rules from the minmax generator
    val numericRuleSet = RuleSet(testDF)
    numericRuleSet.add(numericRules)
    val numericValidationResults = numericRuleSet.validate()

    // Ensure that all ruleTypes are ValidateNumerics
    assert(numericRules.map(_.ruleType == RuleType.ValidateNumerics).reduce(_ && _), "Not every value is validate numerics.")

    // Ensure that there are infinite boundaries, by default
    assert(numericRules.map(_.boundaries.lower == Double.NegativeInfinity).reduce(_ && _), "Lower boundaries are not negatively infinite.")
    assert(numericRules.map(_.boundaries.upper == Double.PositiveInfinity).reduce(_ && _), "Upper boundaries are not positively infinite.")

    // Ensure that the complete report matches expected output
    assert(numericValidationResults.completeReport.exceptAll(numericLovExpectedDF).count() == 0, "Expected numeric df is not equal to the returned rules report.")

    // Ensure that all rows passed the Rules
    assert(numericValidationResults.summaryReport.isEmpty)

    // Ensure rows can be validated against a list of invalid numerics
    val invalidNumColumns = testDF.columns ++ Seq("CheckIfCostIsInLOV", "CheckIfScanPriceIsInLOV", "CheckIfIdIsInLOV")
    val invalidNumsExpectedDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L,
        ValidationValue("Invalid_Price_Rule", passed = true, "[-1.0, -5.0, 0.0, 1000.0]", "2.51"),
        ValidationValue("Invalid_Id_Rule", passed = true, "[7.11111111111111E14, 8.11111111111111E14, 9.11111111111111E14]", "111111111111111"),
        ValidationValue("Invalid_Cost_Rule", passed = true, "[99.0, 10000.0, 100000.0, 1000000.0]", "3")
      ),
      ("food_b", 5.11, 6, 211111111111111L,
        ValidationValue("Invalid_Price_Rule", passed = true, "[-1.0, -5.0, 0.0, 1000.0]", "5.11"),
        ValidationValue("Invalid_Id_Rule", passed = true, "[7.11111111111111E14, 8.11111111111111E14, 9.11111111111111E14]", "211111111111111"),
        ValidationValue("Invalid_Cost_Rule", passed = true, "[99.0, 10000.0, 100000.0, 1000000.0]", "6")
      ),
      ("food_c", 8.22, 99, 311111111111111L,
        ValidationValue("Invalid_Price_Rule", passed = true, "[-1.0, -5.0, 0.0, 1000.0]", "8.22"),
        ValidationValue("Invalid_Id_Rule", passed = true, "[7.11111111111111E14, 8.11111111111111E14, 9.11111111111111E14]", "311111111111111"),
        ValidationValue("Invalid_Cost_Rule", passed = false, "[99.0, 10000.0, 100000.0, 1000000.0]", "99")
      )
    ).toDF(expectedColumns: _*)

    val invalidPrices = Array(-1.00, -5.00, 0.00, 1000.0)
    val invalidIds = Array(711111111111111L, 811111111111111L, 911111111111111L)
    val invalidCosts = Array(99, 10000, 100000, 1000000)
    val invalidNumericalRules = Array(
      Rule("Invalid_Price_Rule", col("scan_price"), invalidPrices, invertMatch = true),
      Rule("Invalid_Id_Rule", col("id"), invalidIds, invertMatch = true),
      Rule("Invalid_Cost_Rule", col("cost"), invalidCosts, invertMatch = true),
    )
    val invalidNumericalResults = RuleSet(testDF).add(invalidNumericalRules).validate()

    // Ensure that there is 1 failed row
    assert(invalidNumericalResults.summaryReport.count() == 1)

    // Ensure that the invertMatch attribute is set properly
    assert(invalidNumericalRules.count(_.invertMatch) == 3)

    // Ensure that the validation report matches expected output
    assert(invalidNumericalResults.completeReport.exceptAll(invalidNumsExpectedDF).count() == 0, "Expected invalid numerics df is not equal to the returned rules report.")

  }

  test("The input df should have no rule failures for valid string LOVs.") {
    val testDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L),
      ("food_b", 5.11, 6, 211111111111111L),
      ("food_c", 8.22, 99, 311111111111111L)
    ).toDF("product_name", "scan_price", "cost", "id")

    // Create a String List of Values Rule
    val validProductNamesRule = Rule("CheckIfProductNameInLOV", col("product_name"), Array("food_a", "food_b", "food_c"))
    val stringIgnoreCaseRule = Rule("IgnoreCaseProductNameLOV", col("product_name"), Array("Food_B", "food_A", "FOOD_C"), ignoreCase = true)
    val invalidFoodsRule = Rule("InvalidProductNameLOV", col("product_name"), Array("food_x", "food_y", "food_z"), invertMatch = true)

    val expectedStringLovColumns = testDF.columns ++ Seq("CheckIfProductNameInLOV", "IgnoreCaseProductNameLOV", "InvalidProductNameLOV")
    val stringLovExpectedDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L,
        ValidationValue("CheckIfProductNameInLOV", passed = true, "[food_a, food_b, food_c]", "food_a"),
        ValidationValue("IgnoreCaseProductNameLOV", passed = true, "[food_b, food_a, food_c]", "food_a"),
        ValidationValue("InvalidProductNameLOV", passed = true, "[food_x, food_y, food_z]", "food_a")
      ),
      ("food_b", 5.11, 6, 211111111111111L,
        ValidationValue("CheckIfProductNameInLOV", passed = true, "[food_a, food_b, food_c]", "food_b"),
        ValidationValue("IgnoreCaseProductNameLOV", passed = true, "[food_b, food_a, food_c]", "food_b"),
        ValidationValue("InvalidProductNameLOV", passed = true, "[food_x, food_y, food_z]", "food_b")
      ),
      ("food_c", 8.22, 99, 311111111111111L,
        ValidationValue("CheckIfProductNameInLOV", passed = true, "[food_a, food_b, food_c]", "food_c"),
        ValidationValue("IgnoreCaseProductNameLOV", passed = true, "[food_b, food_a, food_c]", "food_c"),
        ValidationValue("InvalidProductNameLOV", passed = true, "[food_x, food_y, food_z]", "food_c")
      )
    ).toDF(expectedStringLovColumns: _*)

    // Validate testDF against String LOV Rule
    val productNameRules = Array(validProductNamesRule, stringIgnoreCaseRule, invalidFoodsRule)
    val stringRuleSet = RuleSet(testDF).add(productNameRules)

    val stringValidationResults = stringRuleSet.validate()

    // Ensure that the ruleType is set properly
    assert(validProductNamesRule.ruleType == RuleType.ValidateStrings)

    // Ensure that the complete report matches expected output
    assert(stringValidationResults.completeReport.exceptAll(stringLovExpectedDF).count() == 0, "Expected String LOV df is not equal to the returned rules report.")

    // Ensure that there are infinite boundaries, by default
    assert(validProductNamesRule.boundaries.lower == Double.NegativeInfinity, "Lower boundaries are not negatively infinite.")
    assert(validProductNamesRule.boundaries.upper == Double.PositiveInfinity, "Upper boundaries are not positively infinite.")

    // Ensure that all rows passed; there are no failed rows
    assert(stringValidationResults.summaryReport.isEmpty)
  }

  test("The input df should have no rule failures for an implicit expression rule.") {

    val testDF = Seq(
      (1, "iot_thermostat_1", 84.00, 74.00),
      (2, "iot_thermostat_2", 67.05, 72.00),
      (3, "iot_thermostat_3", 91.14, 76.00)
    ).toDF("device_id", "device_name", "current_temp", "target_temp")

    val expectedColumns = testDF.columns ++ Seq("TemperatureDiffExpressionRule")
    val expectedDF = Seq(
      (1, "iot_thermostat_1", 84.00, 74.00, ValidationValue("TemperatureDiffExpressionRule", passed = true, "(abs((current_temp - target_temp)) < 50.0)", "true")),
      (2, "iot_thermostat_2", 67.05, 72.00, ValidationValue("TemperatureDiffExpressionRule", passed = true, "(abs((current_temp - target_temp)) < 50.0)", "true")),
      (3, "iot_thermostat_3", 91.14, 76.00, ValidationValue("TemperatureDiffExpressionRule", passed = true, "(abs((current_temp - target_temp)) < 50.0)", "true"))
    ).toDF(expectedColumns: _*)

    val exprRuleSet = RuleSet(testDF)
    exprRuleSet.add(Rule("TemperatureDiffExpressionRule", abs(col("current_temp") - col("target_temp")) < 50.00))

    val validationResults = exprRuleSet.validate()

    // Ensure that there are no failed rows for rule expression
    assert(validationResults.summaryReport.isEmpty)

    // Ensure that the ruleType is set correctly
    assert(exprRuleSet.getRules.head.ruleType == RuleType.ValidateExpr)
    assert(exprRuleSet.getRules.head.isImplicitBool)

    // Ensure that the complete report matches the expected output
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected expression df is not equal to the returned rules report.")

  }

  test("The input df should have a single rule failure for an expression rule.") {

    val testDF = Seq(
      (1, "iot_thermostat_1", 84.00, 74.00, -10.00, -10.00),
      (2, "iot_thermostat_2", 76.00, 66.00, -10.00, -10.00),
      (3, "iot_thermostat_3", 91.00, 69.00, -20.00, -10.00)
    ).toDF("device_id", "device_name", "current_temp", "target_temp", "temp_diff", "cooling_rate")

    val expectedColumns = testDF.columns ++ Seq("ImplicitCoolingExpressionRule")
    val expectedDF = Seq(
      (1, "iot_thermostat_1", 84, 74, -10, -10,
        ValidationValue("CoolingExpressionRule", passed = true, "abs(cooling_rate)", "10.0")
      ),
      (2, "iot_thermostat_2", 76, 66, -10, -10,
        ValidationValue("CoolingExpressionRule", passed = true, "abs(cooling_rate)", "10.0")
      ),
      (3, "iot_thermostat_3", 91, 69, -20, -10,
        ValidationValue("CoolingExpressionRule", passed = false, "abs(cooling_rate)", "10.0")
      )
    ).toDF(expectedColumns: _*)

    val exprRuleSet = RuleSet(testDF)
    // Create a rule that ensure the cooling rate can accommodate the temp difference
    exprRuleSet.add(Rule("CoolingExpressionRule", abs(col("cooling_rate")), expr("abs(temp_diff)")))

    val validationResults = exprRuleSet.validate()

    // Ensure that there is a single row failure
    assert(validationResults.summaryReport.count() > 0)
    assert(validationResults.summaryReport.count() == 1)

    // Ensure that the ruleType is set correctly
    assert(exprRuleSet.getRules.head.ruleType == RuleType.ValidateExpr)
    assert(!exprRuleSet.getRules.head.isImplicitBool)

    // Ensure that the complete report matches the expected output
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected explicit expression df is not equal to the returned rules report.")

  }

  test("The input df should have 3 rule failures for complex expression rules.") {

    val testDF = Seq(
      ("Northwest", 1001, 123256, 9.32, 8.99, 4.23, "2021-04-01", "2020-02-01 12:00:00.000"), // bad expiration date
      ("Northwest", 1001, 123456, 19.99, 16.49, 12.99, "2021-07-26", "2020-02-02 12:08:00.000"),
      ("Northwest", 1001, 123456, 0.99, 0.99, 0.10, "2021-07-26", "2020-02-02 12:10:00.000"), // price change too rapid -- same day
      ("Northwest", 1001, 123456, 0.98, 0.90, 0.10, "2021-07-26", "2020-02-05 12:13:00.000"),
      ("Northwest", 1001, 123456, 0.99, 0.99, 0.10, "2021-07-26", "2020-02-07 00:00:00.000"),
      ("Northwest", 1001, 122987, -9.99, -9.49, -6.49, "2021-07-26", "2021-02-01 00:00:00.000"),
    ).toDF("region", "store_id", "sku", "retail_price", "scan_price", "cost", "expiration_date", "create_ts")
      .withColumn("create_ts", 'create_ts.cast("timestamp"))
      .withColumn("create_dt", 'create_ts.cast("date"))

    // Limit price updates to at most one per day
    val window = Window.partitionBy("region", "store_id", "sku").orderBy("create_ts")
    val skuUpdateRule = Rule("One_Update_Per_Day_Rule", unix_timestamp(col("create_ts")) - unix_timestamp(lag("create_ts", 1).over(window)) > 60 * 60 * 24)

    // Limit expiration date to be within a range
    val expirationDateRule = Rule("Expiration_Date_Rule", col("expiration_date").cast("date").between("2021-05-01", "2021-12-31"))

    // Group by region, store_id, sku, expiration_date, create_ts
    val validDatesRuleset = RuleSet(testDF, Array(skuUpdateRule, expirationDateRule), Seq("region", "store_id", "sku", "expiration_date", "create_ts"))
    val validDatesResults = validDatesRuleset.validate()

    // Ensure that there are 2 rule failures
    assert(validDatesResults.summaryReport.count() == 2)
    assert(validDatesResults.completeReport.filter(not(col("One_Update_Per_Day_Rule.passed"))).count() == 1)
    assert(validDatesResults.completeReport.filter(not(col("Expiration_Date_Rule.passed"))).count() == 1)
    assert(validDatesResults.completeReport.filter(not(col("One_Update_Per_Day_Rule.passed"))).select("sku").as[Int].collect.head == 123456)
    assert(validDatesResults.completeReport.filter(not(col("Expiration_Date_Rule.passed"))).select("sku").as[Int].collect.head == 123256)

    // Ensure that the ruleTypes are set correctly
    assert(validDatesRuleset.getRules.count(_.ruleType == RuleType.ValidateExpr) == 2)
    assert(validDatesRuleset.getRules.count(_.isImplicitBool) == 2)
    assert(validDatesRuleset.getGroupBys.length == 5)

    // Limit price columns to be non-negative amounts
    val nonNegativeColumns = array(col("retail_price"), col("scan_price"), col("cost"))
    val nonNegativeValueRule = Rule("Non_Negative_Values_Rule", size(filter(nonNegativeColumns, c => c <= 0.0)) === 0)

    // Group by region, store_id, sku, retail_price, scan_price, cost
    val nonNegativeValuesRuleset = RuleSet(testDF, Array(nonNegativeValueRule), Seq("region", "store_id", "sku", "retail_price", "scan_price", "cost"))
    val nonNegativeValuesResults = nonNegativeValuesRuleset.validate()

    // Ensure that there is 1 rule failure
    assert(nonNegativeValuesResults.summaryReport.count() == 1)
    assert(nonNegativeValuesResults.completeReport.filter(not(col("Non_Negative_Values_Rule.passed"))).count() == 1)
    assert(nonNegativeValuesResults.completeReport.filter(not(col("Non_Negative_Values_Rule.passed"))).select("sku").as[Int].collect.head == 122987)

    // Ensure that the ruleType is set correctly
    assert(nonNegativeValuesRuleset.getRules.head.ruleType == RuleType.ValidateExpr)
    assert(nonNegativeValuesRuleset.getRules.head.isImplicitBool)
    assert(nonNegativeValuesRuleset.getGroupBys.length == 6)

  }

  test("A rule name can have special characters and whitespaces in its name.") {

    val testDF = Seq(
      (1, "iot_thermostat_1", 84.00, 74.00, -10.00, -10.00),
      (2, "iot_thermostat_2", 76.00, 66.00, -10.00, -10.00),
      (3, "iot_thermostat_3", 91.00, 69.00, -20.00, -10.00)
    ).toDF("device_id", "device_name", "current_temp", "target_temp", "temp_diff", "cooling_rate")
    val expectedColumns = testDF.columns ++ Seq("Valid Temperature Range Rule", "!@#$%^&*()--++==%sCooling_Rates~[ ,;{}()\\n\\t=\\\\]+")
    val expectedDF = Seq(
      (1, "iot_thermostat_1", 84.00, 74.00, -10.00, -10.00,
        ValidationValue("Valid Temperature Range Rule", passed=true, "[57.0, 85.0]", "84.0"),
        ValidationValue("!@#$%^&*()--++==%sCooling_Rates~[ ,;{}()\\n\\t=\\\\]+", passed=true, "[-20.0, -1.0]", "-10.0")
      ),
      (2, "iot_thermostat_2", 76.00, 66.00, -10.00, -10.00,
        ValidationValue("Valid Temperature Range Rule", passed=true, "[57.0, 85.0]", "76.0"),
        ValidationValue("!@#$%^&*()--++==%sCooling_Rates~[ ,;{}()\\n\\t=\\\\]+", passed=true, "[-20.0, -1.0]", "-10.0")
      ),
      (3, "iot_thermostat_3", 91.00, 69.00, -20.00, -10.00,
        ValidationValue("Valid Temperature Range Rule", passed=false, "[57.0, 85.0]", "91.0"),
        ValidationValue("!@#$%^&*()--++==%sCooling_Rates~[ ,;{}()\\n\\t=\\\\]+", passed=true, "[-20.0, -1.0]", "-10.0")

      )
    ).toDF(expectedColumns: _*)

    val whiteSpaceRule = Rule("Valid Temperature Range Rule", col("current_temp"), Bounds(57.00, 85.00))
    val specialCharsRule = Rule("!@#$%^&*()--++==%sCooling_Rates~[ ,;{}()\\n\\t=\\\\]+", col("cooling_rate"), Bounds(-20.00, -1.00))
    val whiteSpaceRuleSet = RuleSet(testDF)
        .add(whiteSpaceRule)
        .add(specialCharsRule)
    val validationResults = whiteSpaceRuleSet.validate()

    // Ensure that there is a single temperature rule failure
    assert(validationResults.summaryReport.count() == 1)

    // Ensure that the complete report matches the expected output
    assert(validationResults.completeReport.exceptAll(expectedDF).count() == 0, "Expected special char df is not equal to the returned rules report.")

  }

}
