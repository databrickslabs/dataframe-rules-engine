package com.databricks.labs.validation.utils

import com.databricks.labs.validation.Rule
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Lookups is a handy way to identify categorical values
 * This is meant as an example but does should be rewritten outside of this
 * As of 0.1 release it should either be Array of Int/Long/Double/String
 */
object Lookups {
  final val validStoreIDs = Array(1001, 1002)

  final val validRegions = Array("Northeast", "Southeast", "Midwest", "Northwest", "Southcentral", "Southwest")

  final val validSkus = Array(123456, 122987, 123256, 173544, 163212, 365423, 168212)

  final val invalidSkus = Array(9123456, 9122987, 9123256, 9173544, 9163212, 9365423, 9168212)

}

object Structures {

  case class Bounds(
                     lower: Double = Double.NegativeInfinity,
                     upper: Double = Double.PositiveInfinity,
                     lowerInclusive: Boolean = false,
                     upperInclusive: Boolean = false) {
    def validationLogic(c: Column): Column = {
      val lowerLogic = if (lowerInclusive) c <= lower else c < lower
      val upperLogic = if (upperInclusive) c >= upper else c > upper
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
