package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.Bounds
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite


class RuleTestSuite extends AnyFunSuite with SparkSessionFixture {

  spark.sparkContext.setLogLevel("ERROR")

  test("A MinMaxRule should be instantiated correctly.") {

    val minMaxRule = Rule("Temperature_MinMax_Rule", col("temperature"), Bounds(34.0, 85.0))

    // Ensure that all attributes are set correctly
    assert(minMaxRule.ruleName == "Temperature_MinMax_Rule", "Rule name is not set as expected.")
    assert(minMaxRule.inputColumnName == "temperature", "Input column name is not set as expected.")
    assert(minMaxRule.ruleType == RuleType.ValidateBounds, "The rule type is not set as expected.")
    assert(!minMaxRule.isImplicitBool, "The rule should not be an implicit boolean expression.")
    assert(!minMaxRule.isAgg, "The rule should not be an aggregation.")

    // Ensure that the boundaries are set correctly
    assert(minMaxRule.boundaries.lower == 34.0, "Lower boundary is not set as expected.")
    assert(minMaxRule.boundaries.upper == 85.0, "Upper boundary is not set as expected.")

  }

  test("An implicit boolean expression should be instantiated correctly.") {

    // Ensure a single column of type boolean can be instantiated correctly
    val coolingBoolRule = Rule("Implicit_Cooling_Rule", col("cooling_bool"))

    // Ensure that all attributes are set correctly
    assert(coolingBoolRule.ruleName == "Implicit_Cooling_Rule", "Rule name is not set as expected.")
    assert(coolingBoolRule.inputColumnName == "cooling_bool", "Input column name is not set as expected.")
    assert(coolingBoolRule.ruleType == RuleType.ValidateExpr, "The rule type is not set as expected.")
    assert(coolingBoolRule.isImplicitBool, "The rule should not be an implicit boolean expression.")
    assert(!coolingBoolRule.isAgg, "The rule should not be an aggregation.")

    // Ensure that a boolean expression can be used to create an implicit boolean rule
    val coolingExprRule = Rule("Implicit_Cooling_Expr", col("current_temp") > col("target_temp"))

    // Ensure that all attributes are set correctly
    assert(coolingExprRule.ruleName == "Implicit_Cooling_Expr", "Rule name is not set as expected.")
    assert(coolingExprRule.inputColumnName == "(current_temp > target_temp)", "Input column name is not set as expected.")
    assert(coolingExprRule.ruleType == RuleType.ValidateExpr, "The rule type is not set as expected.")
    assert(coolingExprRule.isImplicitBool, "The rule should not be an implicit boolean expression.")
    assert(!coolingExprRule.isAgg, "The rule should not be an aggregation.")

  }

  test("A column can be ruled equivalent to an expression.") {

    // Ensure that equivalent comparision can be made between a column and expression
    val coolingBoolRule = Rule("Thermostat_Cooling_Rule", col("cooling_bool"), (col("current_temp") - col("target_temp")) >= 7.0)

    // Ensure that all attributes are set correctly
    assert(coolingBoolRule.ruleName == "Thermostat_Cooling_Rule", "Rule name is not set as expected.")
    assert(coolingBoolRule.inputColumnName == "cooling_bool", "Input column name is not set as expected.")
    assert(coolingBoolRule.ruleType == RuleType.ValidateExpr, "The rule type is not set as expected.")
    assert(!coolingBoolRule.isImplicitBool, "The rule should not be an implicit boolean expression.")
    assert(!coolingBoolRule.isAgg, "The rule should not be an aggregation.")

  }

  test("A list of numerical values rule can be instantiated correctly.") {

    // Ensure that a rule with a numerical LOV can be created
    val heatingRateIntRule = Rule("Heating_Rate_Int_Rule", col("heating_rate"), Array(0, 1, 5, 10, 15))

    // Ensure that all attributes are set correctly for Integers
    assert(heatingRateIntRule.ruleName == "Heating_Rate_Int_Rule", "Rule name is not set as expected.")
    assert(heatingRateIntRule.inputColumnName == "heating_rate", "Input column name is not set as expected.")
    assert(heatingRateIntRule.ruleType == RuleType.ValidateNumerics, "The rule type is not set as expected.")
    assert(!heatingRateIntRule.isImplicitBool, "The rule should not be an implicit boolean expression.")
    assert(!heatingRateIntRule.isAgg, "The rule should not be an aggregation.")

    // Ensure that all attributes are set correctly for Doubles
    val heatingRateDoubleRule = Rule("Heating_Rate_Double_Rule", col("heating_rate"), Array(0.0, 0.1, 0.5, 0.10, 0.15))
    assert(heatingRateDoubleRule.ruleName == "Heating_Rate_Double_Rule", "Rule name is not set as expected.")
    assert(heatingRateDoubleRule.inputColumnName == "heating_rate", "Input column name is not set as expected.")
    assert(heatingRateDoubleRule.ruleType == RuleType.ValidateNumerics, "The rule type is not set as expected.")
    assert(!heatingRateDoubleRule.isImplicitBool, "The rule should not be an implicit boolean expression.")
    assert(!heatingRateDoubleRule.isAgg, "The rule should not be an aggregation.")

    // Ensure that all attributes are set correctly for Longs
    val heatingRateLongRule = Rule("Heating_Rate_Long_Rule", col("heating_rate"), Array(111111111111111L, 211111111111111L, 311111111111111L))
    assert(heatingRateLongRule.ruleName == "Heating_Rate_Long_Rule", "Rule name is not set as expected.")
    assert(heatingRateLongRule.inputColumnName == "heating_rate", "Input column name is not set as expected.")
    assert(heatingRateLongRule.ruleType == RuleType.ValidateNumerics, "The rule type is not set as expected.")
    assert(!heatingRateLongRule.isImplicitBool, "The rule should not be an implicit boolean expression.")
    assert(!heatingRateLongRule.isAgg, "The rule should not be an aggregation.")

  }

  test("A list of string values rule can be instantiated correctly.") {

    // Ensure that a rule with a numerical LOV can be created
    val buildingNameRule = Rule("Building_LOV_Rule", col("site_name"), Array("SiteA", "SiteB", "SiteC"))

    // Ensure that all attributes are set correctly for Integers
    assert(buildingNameRule.ruleName == "Building_LOV_Rule", "Rule name is not set as expected.")
    assert(buildingNameRule.inputColumnName == "site_name", "Input column name is not set as expected.")
    assert(buildingNameRule.ruleType == RuleType.ValidateStrings, "The rule type is not set as expected.")
    assert(!buildingNameRule.isImplicitBool, "The rule should not be an implicit boolean expression.")
    assert(!buildingNameRule.isAgg, "The rule should not be an aggregation.")

  }

}
