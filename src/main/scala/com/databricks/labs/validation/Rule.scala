package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.Bounds
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array, lit}

/**
 * Definition of a rule
 */
class Rule(
            private val _ruleName: String,
            val inputColumn: Column,
            val ruleType: RuleType.Value
          ) {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private var _boundaries: Bounds = Bounds()
  private var _validExpr: Column = lit(null)
  private var _validNumerics: Column = array(lit(null).cast("double"))
  private var _validStrings: Column = array(lit(null).cast("string"))
  private var _implicitBoolean: Boolean = false
  private var _ignoreCase: Boolean = false
  private var _invertMatch: Boolean = false
  private val inputRuleName: String = setRuleName(_ruleName)
  val inputColumnName: String = inputColumn.expr.toString().replace("'", "")

  override def toString: String = {
    s"""
       |Rule Name: $ruleName
       |Rule Type: $ruleType
       |Rule Is Agg: $isAgg
       |Input Column: ${inputColumn.expr.toString()}
       |Input Column Name: $inputColumnName
       |Boundaries: ${boundaries.lower} - ${boundaries.upper}
       |Valid Numerics: ${validNumerics.expr.toString()}
       |Valid Strings: ${validStrings.expr.toString()}
       |Implicit Bool: ${_implicitBoolean}
       |""".stripMargin
  }

  private def setBoundaries(value: Bounds): this.type = {
    _boundaries = value
    this
  }

  private def setValidNumerics(value: Array[Double]): this.type = {
    _validNumerics = lit(value)
    this
  }

  private def setValidStrings(value: Array[String], ignoreCase: Boolean): this.type = {
    _validStrings = if(ignoreCase) lit(value.map(_.toLowerCase)) else lit(value)
    inputColumn.expr.children.map(_.prettyName)
    this
  }

  private def setValidExpr(value: Column): this.type = {
    _validExpr = lit(value)
    this
  }

  private def setImplicitBool(value: Boolean): this.type = {
    _implicitBoolean = value
    this
  }

  private def setIgnoreCase(value: Boolean): this.type = {
    _ignoreCase = value
    this
  }

  private def setInvertMatch(value: Boolean): this.type = {
    _invertMatch = value
    this
  }

  private def setRuleName(ruleName: String): String = {
    val removedWhitespaceRuleName = ruleName.trim.replaceAll(" ", "_")
    val whitespaceRemovalWarning = s"Converting whitespaces to underscores in Rule's name:\n '$ruleName' --> '$removedWhitespaceRuleName'\n"
    if (_ruleName.contains(" ")) {
      logger.warn(whitespaceRemovalWarning)
      println(whitespaceRemovalWarning)
    }
    val specialCharsPattern = "[^a-zA-z0-9_-]+".r
    val removedSpecialCharsRuleName = removedWhitespaceRuleName.replaceAll("[^a-zA-Z0-9_-]", "_")
    val specialCharacterRemovalWarning = s"Converting special characters to underscores in Rule's name:\n '$removedWhitespaceRuleName' --> '$removedSpecialCharsRuleName'\n"
    if (specialCharsPattern.findAllIn(removedWhitespaceRuleName).toSeq.nonEmpty) {
      logger.warn(specialCharacterRemovalWarning)
      println(specialCharacterRemovalWarning)
    }
    removedSpecialCharsRuleName
  }

  def boundaries: Bounds = _boundaries

  def validNumerics: Column = _validNumerics

  def validStrings: Column = _validStrings

  def validExpr: Column = _validExpr

  def isImplicitBool: Boolean = _implicitBoolean

  def ignoreCase: Boolean = _ignoreCase

  def invertMatch: Boolean = _invertMatch

  def ruleName: String = inputRuleName

  def isAgg: Boolean = {
    inputColumn.expr.prettyName == "aggregateexpression" ||
      inputColumn.expr.children.map(_.prettyName).contains("aggregateexpression")
  }

}

object Rule {

  /**
   * Several apply methods have been created to handle various types of rules and instantiations from the user
   */

  def apply(
             ruleName: String,
             column: Column,
             boundaries: Bounds
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateBounds)
      .setBoundaries(boundaries)
  }

  def apply(
             ruleName: String,
             column: Column
           ): Rule = {
      apply(ruleName, column, lit(true))
        .setImplicitBool(true)
  }

  def apply(
             ruleName: String,
             column: Column,
             validExpr: Column
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateExpr)
      .setValidExpr(validExpr)
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Double],
             invertMatch: Boolean
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics)
      .setInvertMatch(invertMatch)
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Double]
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics)
      .setInvertMatch(false)
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Long],
             invertMatch: Boolean
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setInvertMatch(invertMatch)
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Long]
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setInvertMatch(false)
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Int],
             invertMatch: Boolean
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setInvertMatch(invertMatch)
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Int]
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setInvertMatch(false)
  }

  def apply(
             ruleName: String,
             column: Column,
             validStrings: Array[String],
             ignoreCase: Boolean = false,
             invertMatch: Boolean = false
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateStrings)
      .setValidStrings(validStrings, ignoreCase)
      .setIgnoreCase(ignoreCase)
      .setInvertMatch(invertMatch)
  }

}
