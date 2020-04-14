package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class ValidatorTestSuite extends org.scalatest.FunSuite with SparkSessionFixture {

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
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
    )

    // Generate the array of Rules from the minmax generator
    val rulesArray = RuleSet.generateMinMaxRules(MinMaxRuleDef("MinMax_Cost_Generated", col("cost"), Bounds(0.0, 12.0)))

    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    someRuleSet.addMinMaxRules("MinMax_Cost_manual", col("cost"), Bounds(0.0,12.0))
    someRuleSet.add(rulesArray)
    val (rulesReport, passed) = someRuleSet.validate()
    rulesReport.show()
    assert(passed)
    assert(rulesReport.count() == 10)
  }

  test("The input dataframe should have exactly 1 rule failure on MinMaxRule") {
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
    val (rulesReport, passed) = someRuleSet.validate()
    val failedResults  = rulesReport.filter(rulesReport("Invalid_Count") > 0).collect()
    assert(failedResults.length == 1)
    assert(failedResults(0)(0) == "MinMax_Cost_max")
    assert(!passed)
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
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99))
    )

    val someRuleSet = RuleSet(testDF, "cost")
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val groupBys = someRuleSet.getGroupBys
    val (groupByValidated, passed) = someRuleSet.validate()

    assert(groupBys.length == 1)
    assert(groupBys.head == "cost")
    assert(someRuleSet.isGrouped)
    assert(passed)
    assert(groupByValidated.count() == 8)
    assert(groupByValidated.filter(groupByValidated("Invalid_Count") > 0).count() == 0)
    assert(groupByValidated.filter(groupByValidated("Failed") === true).count() == 0)
  }

  test("The group by columns are with rules failing the validation") {
    // 2 groups so count of the rules should yield (2 minmax rules * 2 columns) * 2 groups in cost (8 rows)
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 3)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 0.0)),
      MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99))
    )

    val someRuleSet = RuleSet(testDF, "cost")
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val groupBys = someRuleSet.getGroupBys
    val (groupByValidated, passed) = someRuleSet.validate()

    assert(groupBys.length == 1, "Group by length is not 1")
    assert(groupBys.head == "cost", "Group by column is not cost")
    assert(someRuleSet.isGrouped)
    assert(!passed, "Rule set did not fail.")
    assert(groupByValidated.count() == 8, "Rule count should be 8")
    assert(groupByValidated.filter(groupByValidated("Invalid_Count") > 0).count() == 4, "Invalid count is not 4.")
    assert(groupByValidated.filter(groupByValidated("Failed") === true).count() == 4, "Failed count is not 4.")
  }

  test("Validate list of values with numeric types, string types and long types.") {
    val testDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L),
      ("food_b", 5.11, 6, 211111111111111L),
      ("food_c", 8.22, 99, 311111111111111L)
    ).toDF("product_name", "scan_price", "cost", "id")
    val numericRules = Array(
      Rule("CheckIfCostIsInLOV", col("cost"), Array(3,6,99)),
      Rule("CheckIfScanPriceIsInLOV", col("scan_price"), Array(2.51,5.11,8.22)),
      Rule("CheckIfIdIsInLOV", col("id"), Array(111111111111111L,211111111111111L,311111111111111L))
    )
    // Generate the array of Rules from the minmax generator

    val numericRuleSet = RuleSet(testDF)
    numericRuleSet.add(numericRules)
    val (numericValidated, numericPassed) = numericRuleSet.validate()
    assert(numericRules.map(_.ruleType == RuleType.ValidateNumerics).reduce(_ && _), "Not every value is validate numerics.")
    assert(numericRules.map(_.boundaries == null).reduce(_ && _), "Boundaries are not null.")
    assert(numericPassed)
    assert(numericValidated.filter(numericValidated("Invalid_Count") > 0).count() == 0)
    assert(numericValidated.filter(numericValidated("Failed") === true).count() == 0)

    val stringRule = Rule("CheckIfProductNameInLOV", col("product_name"), Array("food_a","food_b","food_c"))
    // Generate the array of Rules from the minmax generator

    val stringRuleSet = RuleSet(testDF)
    stringRuleSet.add(stringRule)
    val (stringValidated, stringPassed) = stringRuleSet.validate()
    assert(stringRule.ruleType == RuleType.ValidateStrings)
    assert(stringRule.boundaries == null)
    assert(stringPassed)
    assert(stringValidated.filter(stringValidated("Invalid_Count") > 0).count() == 0)
    assert(stringValidated.filter(stringValidated("Failed") === true).count() == 0)
  }


}
