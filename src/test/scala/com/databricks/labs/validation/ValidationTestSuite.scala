package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class ValidationTestSuite extends org.scalatest.FunSuite with SparkSessionFixture {

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  test("The input dataframe should have no rule failures on MinMaxRule") {
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

    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val (rulesReport, passed) = someRuleSet.validate()
    assert(passed == true)
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
    assert(passed == false)
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
    assert(groupBys(0) == "cost")
    assert(someRuleSet.isGrouped == true)
    assert(passed == true)
    assert(groupByValidated.count() == 8)
    assert(groupByValidated.filter(groupByValidated("Invalid_Count") > 0).count() == 0)
    assert(groupByValidated.filter(groupByValidated("Failed") === true).count() == 0)
  }

  test("Validate list of values for numerics.") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 99)
    ).toDF("retail_price", "scan_price", "cost")
    val rule = Rule("CheckIfCostIsInLOV", col("cost"), Array(3,6,99))
    // Generate the array of Rules from the minmax generator

    val someRuleSet = RuleSet(testDF)
    someRuleSet.add(rule)
    assert(rule.ruleType == RuleType.ValidateNumerics)
//    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
//    val (rulesReport, passed) = someRuleSet.validate()
//    val failedResults  = rulesReport.filter(rulesReport("Invalid_Count") > 0).collect()
//    assert(failedResults.length == 1)
//    assert(failedResults(0)(0) == "MinMax_Cost_max")
//    assert(passed == false)
  }

}
