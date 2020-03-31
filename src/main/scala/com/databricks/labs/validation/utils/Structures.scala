package com.databricks.labs.validation.utils

import org.apache.spark.sql.Column

object Structures {

  case class Bounds(lower: Double = Double.NegativeInfinity, upper: Double = Double.PositiveInfinity)

  case class Result(ruleName: String, colName: String, funcname: String, boundaries: Bounds, actualVal: Double, passed: Boolean)

  case class MinMaxRuleDef(ruleName: String, column: Column, bounds: Bounds, by: Column*)

  case class ValidNumerics(columnName: String, validNumerics: Array[Double])
  case class ValidStrings(columnName: String, validStrings: Array[String])

  final val validNumerics = ValidNumerics("store_id", Array(1001.0, 1002.0))

  final val validStrings = ValidStrings("region",
    Array("Northeast", "Southeast", "Midwest", "Northwest", "Southcentral", "Southwest")
  )

}
