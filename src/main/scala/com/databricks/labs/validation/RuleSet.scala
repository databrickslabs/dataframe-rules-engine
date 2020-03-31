package com.databricks.labs.validation

import com.databricks.labs.validation.utils.{MinMaxFunc, SparkSessionWrapper}
import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef, Result}
import com.databricks.labs.validation.utils.Helpers._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.MetadataBuilder

import scala.collection.mutable.ArrayBuffer

class RuleSet extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private var _df: Any = _
  private var _isGrouped: Boolean = _
  private val rulesReport = ArrayBuffer[Result]()
  private val _rules = ArrayBuffer[Rule]()

  private def setDF(value: Any): this.type = {
    _df = value
    this
  }

  private def setIsGrouped(value: Boolean): this.type = {
    _isGrouped = value
    this
  }

  private def getDf: Any = _df
  private def getGroupedFlag: Boolean = _isGrouped
  private def getRules: Array[Rule] = _rules.toArray


  def addMinMaxRules(ruleName: String,
                     inputColumn: Column,
                     boundaries: Bounds,
                     by: Column*
                    ): this.type = {
    val rules = Array(
      Rule(ruleName, inputColumn, functions.min, s"${getColumnName(inputColumn)}_min",
        boundaries, by: _*),
      Rule(ruleName, inputColumn, functions.max, s"${getColumnName(inputColumn)}_max",
        boundaries, by: _*)
    )
    add(rules)
  }

  def add(rules: Seq[Rule]): this.type = {
    rules.foreach(rule => _rules.append(rule))
    this
  }

  def add(rule: Rule): this.type = {
    _rules.append(rule)
    this
  }

  def add(ruleSet: RuleSet): RuleSet = {
    new RuleSet().setDF(ruleSet.getDf)
      .setIsGrouped(ruleSet.getGroupedFlag)
      .add(ruleSet.getRules)
  }

  /**
   * Logic to actually test compliance with provided rules added through the builder
   *
   * @return this but is marked private
   */
  private def applyValidation: this.type = {

    // Logic application
    val rules = _rules.toArray
    val actuals = rules.map(rule => rule.aggFunc(rule.inputColumn).cast("double").alias(rule.alias))

    // Result after applying the logic
    val results = if (_isGrouped) {
      _df.asInstanceOf[RelationalGroupedDataset].agg(actuals.head, actuals.tail: _*).first()
    } else {
      _df.asInstanceOf[DataFrame].select(actuals: _*).first()
    }


    // Test to see if results are compliant with the rules
    // Result is appended to the case class Result
    rules.foreach(rule => {
      val funcRaw = rule.aggFunc.apply(rule.inputColumn).toString()
      val funcName = funcRaw.substring(0, funcRaw.indexOf("("))
      val actVal = results.getDouble(results.fieldIndex(rule.alias))
      rulesReport.append(Result(rule.ruleName, rule.alias, funcName, rule.boundaries,
        actVal, actVal < rule.boundaries.upper && actVal > rule.boundaries.lower))
    })
    this
  }

  /**
   * Call the action once all rules have been applied
   * @return Tuple of Dataframe report and final boolean of whether all rules were passed
   */
  def validate: (DataFrame, Boolean) = {
    applyValidation
    val reportDF = rulesReport.toDS.toDF
    (reportDF,
      reportDF.filter('passed === false).count > 0)
  }

}

object RuleSet {

  /**
   * Accept either a regular DataFrame or a Grouped DataFrame as the base
   * Either append rule[s] at call or via builder pattern
   */

  def apply(df: DataFrame): RuleSet = {
    new RuleSet().setDF(df).setIsGrouped(false)
  }

  def apply(df: RelationalGroupedDataset): RuleSet = {
    new RuleSet().setDF(df).setIsGrouped(true)
  }

  def apply(df: DataFrame, rules: Rule*): RuleSet = {
    new RuleSet().setDF(df).setIsGrouped(false)
      .add(rules)
  }

  def apply(df: RelationalGroupedDataset, rules: Rule*): RuleSet = {
    new RuleSet().setDF(df).setIsGrouped(true)
      .add(rules)
  }

  def generateMinMaxRules(minMaxRuleDefs: MinMaxRuleDef*): Array[Rule] = {

    minMaxRuleDefs.flatMap(ruleDef => {
      Seq(
        Rule(ruleDef.ruleName, ruleDef.column, functions.min,
          s"${getColumnName(ruleDef.column)}_min", ruleDef.bounds, ruleDef.by: _*),
        Rule(ruleDef.ruleName, ruleDef.column, functions.max,
          s"${getColumnName(ruleDef.column)}_min", ruleDef.bounds, ruleDef.by: _*)
      )
    }).toArray
  }

}