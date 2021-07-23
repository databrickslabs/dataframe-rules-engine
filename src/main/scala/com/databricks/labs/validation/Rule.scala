package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.Bounds
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, array}

import java.util.UUID

/**
 * Definition of a rule
 */
class Rule(
            val ruleName: String,
            val inputColumn: Column,
            val ruleType: RuleType.Value
          ) {

  private var _boundaries: Bounds = Bounds()
  private var _validExpr: Column = lit(null)
  private var _validNumerics: Column = array(lit(null).cast("double"))
  private var _validStrings: Column = array(lit(null).cast("string"))
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

  private def setValidStrings(value: Array[String]): this.type = {
    _validStrings = lit(value)
    inputColumn.expr.children.map(_.prettyName)
    this
  }

  private def setValidExpr(value: Column): this.type = {
    _validExpr = lit(value)
    this
  }

  def boundaries: Bounds = _boundaries

  def validNumerics: Column = _validNumerics

  def validStrings: Column = _validStrings

  def validExpr: Column = _validExpr

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
             column: Column,
             validExpr: Column
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateExpr)
      .setValidExpr(validExpr)
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Double]
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics)
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Long]
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Int]
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateNumerics)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
  }

  def apply(
             ruleName: String,
             column: Column,
             validStrings: Array[String]
           ): Rule = {

    new Rule(ruleName, column, RuleType.ValidateStrings)
      .setValidStrings(validStrings)
  }

}
