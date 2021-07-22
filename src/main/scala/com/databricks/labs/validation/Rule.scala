package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.Bounds
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import java.util.UUID

/**
 * Definition of a rule
 */
class Rule {

  private var _ruleName: String = _
  private var _canonicalCol: Column = _
  private var _canonicalColName: String = _
  private var _inputCol: Column = _
  private var _inputColName: String = _
  private var _calculatedColumn: Column = _
  private var _validExpr: Column = lit(null)
  private var _boundaries: Bounds = _
  private var _validNumerics: Column = _
  private var _validStrings: Column = _
  private var _dateTimeLogic: Column = _
  private var _ruleType: RuleType.Value = _
  private var _isAgg: Boolean = _

  private def setRuleName(value: String): this.type = {
    _ruleName = value
    this
  }

  /**
   * Allows for use of canonical naming and rule identification. Not necessary as of version 0.1 but
   * can be used for future use cases
   *
   * @param value input column from user
   * @return Rule
   */
  private[validation] def setColumn(value: Column): this.type = {
    _inputCol = value
    _inputColName = _inputCol.expr.toString().replace("'", "")
    val cleanUUID = UUID.randomUUID().toString.replaceAll("-", "")
    _canonicalColName = s"${_inputColName}_$cleanUUID"
    _canonicalCol = _inputCol.alias(_canonicalColName)
    _calculatedColumn = _inputCol
    this
  }

  private[validation] def setCalculatedColumn(value: Column): Unit = {
    _calculatedColumn = value
  }

  private def setBoundaries(value: Bounds): this.type = {
    _boundaries = value
    this
  }

  private def setValidExpr(value: Column): this.type = {
    _validExpr = value
    this
  }

  private def setValidNumerics(value: Array[Double]): this.type = {
    _validNumerics = lit(value)
    this
  }

  private def setValidStrings(value: Array[String]): this.type = {
    _validStrings = lit(value)
    this
  }

  private def setDateTimeLogic(value: Column): this.type = {
    _dateTimeLogic = value
    this
  }

  private def setRuleType(value: RuleType.Value): this.type = {
    _ruleType = value
    this
  }

  private[validation] def setIsAgg: this.type = {
    _isAgg = inputColumn.expr.prettyName == "aggregateexpression"
    this
  }

  def ruleName: String = _ruleName

  def inputColumn: Column = _inputCol

  def inputColumnName: String = _inputColName

  def canonicalCol: Column = _canonicalCol

  def canonicalColName: String = _canonicalColName

  private[validation] def calculatedColumn: Column = _calculatedColumn

  def boundaries: Bounds = _boundaries

  def validExpr: Column = _validExpr

  def validNumerics: Column = _validNumerics

  def validStrings: Column = _validStrings

  def dateTimeLogic: Column = _dateTimeLogic

  def ruleType: RuleType.Value = _ruleType

  private[validation] def isAgg: Boolean = _isAgg

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

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setBoundaries(boundaries)
      .setRuleType(RuleType.ValidateBounds)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validationExpr: Column
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidExpr(validationExpr)
      .setRuleType(RuleType.ValidateExpr)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Double]
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics)
      .setRuleType(RuleType.ValidateNumerics)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Long]
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setRuleType(RuleType.ValidateNumerics)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validNumerics: Array[Int]
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidNumerics(validNumerics.map(_.toString.toDouble))
      .setRuleType(RuleType.ValidateNumerics)
      .setIsAgg
  }

  def apply(
             ruleName: String,
             column: Column,
             validStrings: Array[String]
           ): Rule = {

    new Rule()
      .setRuleName(ruleName)
      .setColumn(column)
      .setValidStrings(validStrings)
      .setRuleType(RuleType.ValidateStrings)
      .setIsAgg
  }

  /**
   * TODO -- Implement Date/Time Logic for:
   * Column Type (i.e. current_timestamp and current_date)
   * java.util.Date
   * Validated strings compatible with Spark
   *
   * Additional logic can be added to extend functionality
   */

  //  def apply(
  //             ruleName: String,
  //             column: Column,
  //             dateTimeLogic: ???,
  //           ): Rule = {
  //
  //    new Rule()
  //      .setRuleName(ruleName)
  //      .setColumn(column)
  //      .setAggFunc(aggFunc)
  //      .setAlias(alias)
  //      .setDateTimeLogic(dateTimeLogic)
  //      .setRuleType("dateTime")
  //      .setByCols(by)
  //  }

}
