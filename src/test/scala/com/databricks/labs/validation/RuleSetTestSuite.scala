package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.Bounds
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite


class RuleSetTestSuite extends AnyFunSuite with SparkSessionFixture {

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  test("A rule set should be created from a DataFrame.") {
    val testDF = Seq(
      (1, 2, 3),
      (4, 5, 6),
      (7, 8, 9)
    ).toDF("retail_price", "scan_price", "cost")
    val testRuleSet = RuleSet(testDF)

    // Ensure that the RuleSet DataFrame is set properly
    assert(testRuleSet.getDf.exceptAll(testDF).count() == 0, "RuleSet DataFrame is not equal to the input DataFrame.")

    // Ensure that the RuleSet properties are set properly
    assert(!testRuleSet.isGrouped)
    assert(testRuleSet.getGroupBys.isEmpty)
    assert(testRuleSet.getRules.isEmpty)

  }

  test("A rule set should be created from a DataFrame grouped by multiple columns.") {
    val testDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L),
      ("food_b", 5.11, 6, 211111111111111L),
      ("food_b", 5.32, 7, 311111111111111L),
      ("food_d", 8.22, 99, 411111111111111L)
    ).toDF("product_name", "scan_price", "cost", "id")
    val testRuleSet = RuleSet(testDF, Array("product_name", "id"))

    // Ensure that the RuleSet DataFrame is set properly
    assert(testRuleSet.getDf.exceptAll(testDF).count() == 0, "RuleSet DataFrame is not equal to the input DataFrame.")

    // Ensure that the group-by columns are set properly
    assert(testRuleSet.isGrouped)
    assert(testRuleSet.getGroupBys.length == 2)
    assert(testRuleSet.getGroupBys.contains("product_name"))
    assert(testRuleSet.getGroupBys.contains("id"))

    // Ensure that the RuleSet properties are set properly
    assert(testRuleSet.getRules.isEmpty)

  }

  test("A rule set should be created from a DataFrame grouped by a single column.") {
    val testDF = Seq(
      ("food_a", 2.51, 3, 111111111111111L),
      ("food_b", 5.11, 6, 211111111111111L),
      ("food_b", 5.32, 7, 311111111111111L),
      ("food_d", 8.22, 99, 411111111111111L)
    ).toDF("product_name", "scan_price", "cost", "id")
    val testRuleSet = RuleSet(testDF, "product_name")

    // Ensure that the RuleSet DataFrame is set properly
    assert(testRuleSet.getDf.exceptAll(testDF).count() == 0, "RuleSet DataFrame is not equal to the input DataFrame.")

    // Ensure that the group-by columns are set properly
    assert(testRuleSet.isGrouped)
    assert(testRuleSet.getGroupBys.length == 1)
    assert(testRuleSet.getGroupBys.head == "product_name")

    // Ensure that the RuleSet properties are set properly
    assert(testRuleSet.getRules.isEmpty)

  }

  test("A rule set should be created from a DataFrame and list of rules.") {
    val testDF = Seq(
      ("Toyota", "Camry", 30000.00, 111111111111111L),
      ("Ford", "Escape", 18750.00, 211111111111111L),
      ("Ford", "Mustang", 32000.00, 311111111111111L),
      ("Nissan", "Maxima", 25000.00, 411111111111111L)
    ).toDF("make", "model", "msrp", "id")
    val makeLovRule = Rule("Valid_Auto_Maker_Rule", col("make"), Array("Ford", "Toyota", "Nissan", "BMW", "Chevrolet"))
    val modelLovRule = Rule("Valid_Auto_Models_Rule", col("model"), Array("Camry", "Mustang", "Maxima", "Escape", "330i"))
    val groupedRuleSet = RuleSet(testDF, Array(makeLovRule, modelLovRule), Array("make"))

    // Ensure that the RuleSet DataFrame is set properly
    assert(groupedRuleSet.getDf.exceptAll(testDF).count() == 0, "RuleSet DataFrame is not equal to the input DataFrame.")

    // Ensure that the RuleSet properties are set properly
    assert(groupedRuleSet.isGrouped)
    assert(groupedRuleSet.getGroupBys.length == 1)
    assert(groupedRuleSet.getGroupBys.head == "make")
    assert(groupedRuleSet.getRules.length == 2)
    assert((groupedRuleSet.getRules.map(_.ruleName) diff Seq("Valid_Auto_Maker_Rule", "Valid_Auto_Models_Rule")).isEmpty)

    // Ensure a RuleSet can be created with a non-grouped DataFrame
    val nonGroupedRuleSet = RuleSet(testDF, Array(makeLovRule, modelLovRule))

    // Ensure that the RuleSet DataFrame is set properly
    assert(nonGroupedRuleSet.getDf.exceptAll(testDF).count() == 0, "RuleSet DataFrame is not equal to the input DataFrame.")

    // Ensure that the RuleSet properties are set properly
    assert(nonGroupedRuleSet.getGroupBys.isEmpty)
    assert(nonGroupedRuleSet.getRules.length == 2)
    assert((nonGroupedRuleSet.getRules.map(_.ruleName) diff Seq("Valid_Auto_Maker_Rule", "Valid_Auto_Models_Rule")).isEmpty)
  }

  test("A rule set should be created from a DataFrame and list of MinMax rules.") {
    val testDF = Seq(
      ("Toyota", "Camry", 30000.00, 111111111111111L),
      ("Ford", "Escape", 18750.00, 211111111111111L),
      ("Ford", "Mustang", 32000.00, 311111111111111L),
      ("Nissan", "Maxima", 25000.00, 411111111111111L)
    ).toDF("make", "model", "msrp", "id")
    val msrpBoundsRuleSet = RuleSet(testDF).addMinMaxRules("Valid_Auto_MSRP_Rule", col("msrp"), Bounds(1.0, 100000.0))

    // Ensure that the RuleSet DataFrame is set properly
    assert(msrpBoundsRuleSet.getDf.exceptAll(testDF).count() == 0, "RuleSet DataFrame is not equal to the input DataFrame.")

    // Ensure that the RuleSet properties are set properly
    assert(msrpBoundsRuleSet.getGroupBys.isEmpty)
    assert(msrpBoundsRuleSet.getRules.length == 2)
    assert(Seq("Valid_Auto_MSRP_Rule_min", "Valid_Auto_MSRP_Rule_max").contains(msrpBoundsRuleSet.getRules(0).ruleName))
    assert(Seq("Valid_Auto_MSRP_Rule_min", "Valid_Auto_MSRP_Rule_max").contains(msrpBoundsRuleSet.getRules(1).ruleName))

  }

  test("Two rule sets can be merged together.") {

    val testDF = Seq(
      ("Toyota", "Camry", 30000.00, 111111111111111L),
      ("Ford", "Escape", 18750.00, 211111111111111L),
      ("Ford", "Mustang", 32000.00, 311111111111111L),
      ("Nissan", "Maxima", 25000.00, 411111111111111L)
    ).toDF("make", "model", "msrp", "id")

    // Create a bounds RuleSet
    val msrpBoundsRuleSet = RuleSet(testDF).addMinMaxRules("Valid_Auto_MSRP_Rule", col("msrp"), Bounds(1.0, 100000.0))

    // Create a LOV RuleSet
    val makeLovRule = Rule("Valid_Auto_Maker_Rule", col("make"), Array("Ford", "Toyota", "Nissan", "BMW", "Chevrolet"))
    val modelLovRule = Rule("Valid_Auto_Models_Rule", col("model"), Array("Camry", "Mustang", "Maxima", "Escape", "330i"))
    val groupedRuleSet = RuleSet(testDF, Array(makeLovRule, modelLovRule), Array("make"))

    // Merge both RuleSets
    val mergedRuleSet = groupedRuleSet.add(msrpBoundsRuleSet)

    // Ensure that the RuleSet DataFrame is set properly
    assert(mergedRuleSet.getGroupBys.length == 1)
    assert(mergedRuleSet.getDf.exceptAll(testDF).count() == 0, "RuleSet DataFrame is not equal to the input DataFrame.")

    // Ensure that the RuleSet properties are set properly
    assert(mergedRuleSet.getRules.length == 4)
    val mergedRuleNames = Seq("Valid_Auto_MSRP_Rule_min", "Valid_Auto_MSRP_Rule_max", "Valid_Auto_Maker_Rule", "Valid_Auto_Models_Rule")
    assert(mergedRuleSet.getRules.count(r => mergedRuleNames.contains(r.ruleName)) == 4)

    // Ensure groupBy columns are merged properly
    val groupedLovRuleSet = RuleSet(testDF, Array(makeLovRule, modelLovRule), Array("make"))
    val mergedTheOtherWay = msrpBoundsRuleSet.add(groupedLovRuleSet)
    assert(mergedTheOtherWay.getGroupBys.length == 1)
    assert(mergedTheOtherWay.getGroupBys.head == "make")
    assert(mergedTheOtherWay.getDf.exceptAll(testDF).count() == 0)
    mergedTheOtherWay.getRules.map(_.ruleName).foreach(println)
    assert(mergedTheOtherWay.getRules.count(r => mergedRuleNames.contains(r.ruleName)) == 4)

  }

}
