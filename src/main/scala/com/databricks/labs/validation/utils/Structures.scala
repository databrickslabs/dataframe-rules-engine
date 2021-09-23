package com.databricks.labs.validation.utils

import com.databricks.labs.validation.Rule
import org.apache.spark.sql.{Column, DataFrame}

object Structures {

  case class Bounds(
                     lower: Double = Double.NegativeInfinity,
                     upper: Double = Double.PositiveInfinity,
                     lowerInclusive: Boolean = false,
                     upperInclusive: Boolean = false) {
    def validationLogic(c: Column): Column = {
      val lowerLogic = if (lowerInclusive) c >= lower else c > lower
      val upperLogic = if (upperInclusive) c <= upper else c < upper
      lowerLogic && upperLogic
    }
  }

  case class MinMaxRuleDef(ruleName: String, column: Column, bounds: Bounds, by: Column*)

  case class ValidationResults(completeReport: DataFrame, summaryReport: DataFrame)

  private[validation] class ValidationException(s: String) extends Exception(s) {}

  private[validation] class InvalidRuleException(r: Rule, s: String) extends Exception(s) {
    val msg: String = s"RULE VALIDATION FAILED: ${r.toString}"
    throw new ValidationException(msg)
  }

}
