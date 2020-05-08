package com.databricks.labs.validation

/**
 * Definition of the Rule Types as an Enumeration for better type matching
 */
object RuleType extends Enumeration {
  val ValidateBounds = Value("bounds")
  val ValidateNumerics = Value("validNumerics")
  val ValidateStrings = Value("validStrings")
  val ValidateDateTime = Value("validDateTime")
  val ValidateComplex = Value("complex")
}
