package com.databricks.labs.validation.utils

import org.apache.spark.sql.Column

/**
 * Lookups is a handy way to identify categorical values
 * This is meant as an example but does should be rewritten outside of this
 * As of 0.1 release it should either be Array of Int/Long/Double/String
 */
object Lookups {
  final val validStoreIDs = Array(1001, 1002)

  final val validRegions = Array("Northeast", "Southeast", "Midwest", "Northwest", "Southcentral", "Southwest")

  final val validSkus = Array(123456, 122987,123256, 173544, 163212, 365423, 168212)

}

object Structures {

  case class Bounds(lower: Double = Double.NegativeInfinity, upper: Double = Double.PositiveInfinity)

  case class MinMaxRuleDef(ruleName: String, column: Column, bounds: Bounds, by: Column*)

}
