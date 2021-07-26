package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef}
import org.apache.spark.sql.functions.{col, min}
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

  test("Validate list of values with numeric types, string types and long types.") {
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

    // Create a String List of Values Rule
    val stringRule = Rule("CheckIfProductNameInLOV", col("product_name"), Array("food_a", "food_b", "food_c"))

    val expectedStringLovColumns = testDF.columns ++ Seq("CheckIfProductNameInLOV")
    val stringLovExpectedDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L,
        ValidationValue("CheckIfProductNameInLOV", passed = true, "[food_a, food_b, food_c]", "food_a")
      ),
      ("food_b", 5.11, 6, 211111111111111L,
        ValidationValue("CheckIfProductNameInLOV", passed = true, "[food_a, food_b, food_c]", "food_b")
      ),
      ("food_c", 8.22, 99, 311111111111111L,
        ValidationValue("CheckIfProductNameInLOV", passed = true, "[food_a, food_b, food_c]", "food_c")
      )
    ).toDF(expectedStringLovColumns: _*)

    // Validate testDF against String LOV Rule
    val stringRuleSet = RuleSet(testDF)
    stringRuleSet.add(stringRule)
    val stringValidationResults = stringRuleSet.validate()

    // Ensure that the ruleType is set properly
    assert(stringRule.ruleType == RuleType.ValidateStrings)

    // Ensure that the complete report matches expected output
    assert(stringValidationResults.completeReport.exceptAll(stringLovExpectedDF).count() == 0, "Expected String LOV df is not equal to the returned rules report.")

    // Ensure that there are infinite boundaries, by default
    assert(stringRule.boundaries.lower == Double.NegativeInfinity, "Lower boundaries are not negatively infinite.")
    assert(stringRule.boundaries.upper == Double.PositiveInfinity, "Upper boundaries are not positively infinite.")

    // Ensure that all rows passed; there are no failed rows
    assert(stringValidationResults.summaryReport.isEmpty)
  }

}
