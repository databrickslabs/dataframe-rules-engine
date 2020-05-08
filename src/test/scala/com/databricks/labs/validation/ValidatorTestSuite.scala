package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef}
import org.apache.spark.sql.functions.{col, min}

case class ValidationValue(validDateTime: java.lang.Long, validNumerics: Array[Double], bounds: Array[Double], validStrings: Array[String])

class ValidatorTestSuite extends org.scalatest.FunSuite with SparkSessionFixture {

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  test("The input dataframe should have no rule failures on MinMaxRule") {
    val expectedDF = Seq(
      ("MinMax_Cost_Generated_max","bounds",ValidationValue(null,null,Array(0.0, 12.0),null),0,false),
      ("MinMax_Cost_Generated_min","bounds",ValidationValue(null,null,Array(0.0, 12.0),null),0,false),
      ("MinMax_Cost_manual_max","bounds",ValidationValue(null,null,Array(0.0, 12.0),null),0,false),
      ("MinMax_Cost_manual_min","bounds",ValidationValue(null,null,Array(0.0, 12.0),null),0,false),
      ("MinMax_Cost_max","bounds",ValidationValue(null,null,Array(0.0, 12.0),null),0,false),
      ("MinMax_Cost_min","bounds",ValidationValue(null,null,Array(0.0, 12.0),null),0,false),
      ("MinMax_Scan_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      ("MinMax_Scan_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      ("MinMax_Sku_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      ("MinMax_Sku_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false)
      ).toDF("Rule_Name","Rule_Type","Validation_Values","Invalid_Count","Failed")
    val data = Seq()
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
    assert(rulesReport.except(expectedDF).count() == 0)
    assert(passed)
    assert(rulesReport.count() == 10)
  }

  test("The input rule should have 1 invalid count for MinMax_Scan_Price_Minus_Retail_Price_min and max for failing complex type.") {
    val expectedDF = Seq(
      ("MinMax_Retail_Price_Minus_Scan_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),1,true),
      ("MinMax_Retail_Price_Minus_Scan_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),1,true),
      ("MinMax_Scan_Price_Minus_Retail_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      ("MinMax_Scan_Price_Minus_Retail_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false)
    ).toDF("Rule_Name","Rule_Type","Validation_Values","Invalid_Count","Failed")

    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Array(
      MinMaxRuleDef("MinMax_Retail_Price_Minus_Scan_Price", col("retail_price")-col("scan_price"), Bounds(0.0, 29.99)),
      MinMaxRuleDef("MinMax_Scan_Price_Minus_Retail_Price", col("scan_price")-col("retail_price"), Bounds(0.0, 29.99))
    )

    // Generate the array of Rules from the minmax generator
    val someRuleSet = RuleSet(testDF)
    someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
    val (rulesReport, passed) = someRuleSet.validate()
    assert(rulesReport.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(!passed)
    assert(rulesReport.count() == 4)
  }

  test("The input rule should have 3 invalid count for failing aggregate type.") {
    val expectedDF = Seq(
      ("MinMax_Min_Retail_Price","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      ("MinMax_Min_Scan_Price","bounds",ValidationValue(null,null,Array(3.0, 29.99),null),1,true)
    ).toDF("Rule_Name","Rule_Type","Validation_Values","Invalid_Count","Failed")
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")
    val minMaxPriceDefs = Seq(
      Rule("MinMax_Min_Retail_Price", min("retail_price"), Bounds(0.0, 29.99)),
      Rule("MinMax_Min_Scan_Price", min("scan_price"), Bounds(3.0, 29.99))
    )


    // Generate the array of Rules from the minmax generator
    val someRuleSet = RuleSet(testDF)
    someRuleSet.add(minMaxPriceDefs)
    val (rulesReport, passed) = someRuleSet.validate()
    assert(rulesReport.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(!passed)
    assert(rulesReport.count() == 2)
  }

  test("The input dataframe should have exactly 1 rule failure on MinMaxRule") {
    val expectedDF = Seq(
      ("MinMax_Cost_max","bounds",ValidationValue(null,null,Array(0.0, 12.00),null),1,true),
      ("MinMax_Cost_min","bounds",ValidationValue(null,null,Array(0.0, 12.00),null),0,false),
      ("MinMax_Scan_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      ("MinMax_Scan_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      ("MinMax_Sku_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      ("MinMax_Sku_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false)
    ).toDF("Rule_Name","Rule_Type","Validation_Values","Invalid_Count","Failed")
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
    assert(rulesReport.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
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
    val expectedDF = Seq(
      (3,"MinMax_Scan_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (6,"MinMax_Scan_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (3,"MinMax_Scan_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (6,"MinMax_Scan_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (3,"MinMax_Sku_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (6,"MinMax_Sku_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (3,"MinMax_Sku_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (6,"MinMax_Sku_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false)
    ).toDF("cost","Rule_Name","Rule_Type","Validation_Values","Invalid_Count","Failed")
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
    assert(groupByValidated.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(groupByValidated.filter(groupByValidated("Invalid_Count") > 0).count() == 0)
    assert(groupByValidated.filter(groupByValidated("Failed") === true).count() == 0)
  }

  test("The group by columns are with rules failing the validation") {
    val expectedDF = Seq(
      (3,"MinMax_Sku_Price_max","bounds",ValidationValue(null,null,Array(0.0, 0.0),null),1,true),
      (6,"MinMax_Sku_Price_max","bounds",ValidationValue(null,null,Array(0.0, 0.0),null),1,true),
      (3,"MinMax_Sku_Price_min","bounds",ValidationValue(null,null,Array(0.0, 0.0),null),1,true),
      (6,"MinMax_Sku_Price_min","bounds",ValidationValue(null,null,Array(0.0, 0.0),null),1,true),
      (3,"MinMax_Scan_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (6,"MinMax_Scan_Price_max","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (3,"MinMax_Scan_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false),
      (6,"MinMax_Scan_Price_min","bounds",ValidationValue(null,null,Array(0.0, 29.99),null),0,false)
    ).toDF("cost","Rule_Name","Rule_Type","Validation_Values","Invalid_Count","Failed")
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
    assert(groupByValidated.except(expectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(groupByValidated.filter(groupByValidated("Invalid_Count") > 0).count() == 4, "Invalid count is not 4.")
    assert(groupByValidated.filter(groupByValidated("Failed") === true).count() == 4, "Failed count is not 4.")
  }

  test("Validate list of values with numeric types, string types and long types.") {

    val testDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L),
      ("food_b", 5.11, 6, 211111111111111L),
      ("food_c", 8.22, 99, 311111111111111L)
    ).toDF("product_name", "scan_price", "cost", "id")

    val numericLovExpectedDF = Seq(
      ("CheckIfCostIsInLOV","validNumerics",ValidationValue(null,Array(3,6,99),null,null),0,false),
      ("CheckIfScanPriceIsInLOV","validNumerics",ValidationValue(null,Array(2.51,5.11,8.22),null,null),0,false),
      ("CheckIfIdIsInLOV","validNumerics",ValidationValue(null,Array(111111111111111L,211111111111111L,311111111111111L),null,null),0,false)
    ).toDF("Rule_Name","Rule_Type","Validation_Values","Invalid_Count","Failed")
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
    assert(numericValidated.except(numericLovExpectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(numericValidated.filter(numericValidated("Invalid_Count") > 0).count() == 0)
    assert(numericValidated.filter(numericValidated("Failed") === true).count() == 0)

    val stringRule = Rule("CheckIfProductNameInLOV", col("product_name"), Array("food_a","food_b","food_c"))
    // Generate the array of Rules from the minmax generator

    val stringLovExpectedDF = Seq(
      ("CheckIfProductNameInLOV","validStrings",ValidationValue(null,null,null,Array("food_a", "food_b", "food_c")),0,false)
    ).toDF("Rule_Name","Rule_Type","Validation_Values","Invalid_Count","Failed")

    val stringRuleSet = RuleSet(testDF)
    stringRuleSet.add(stringRule)
    val (stringValidated, stringPassed) = stringRuleSet.validate()
    assert(stringRule.ruleType == RuleType.ValidateStrings)
    assert(stringRule.boundaries == null)
    assert(stringPassed)
    assert(stringValidated.except(stringLovExpectedDF).count() == 0, "Expected df is not equal to the returned rules report.")
    assert(stringValidated.filter(stringValidated("Invalid_Count") > 0).count() == 0)
    assert(stringValidated.filter(stringValidated("Failed") === true).count() == 0)
  }


}
