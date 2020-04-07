package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.{Bounds, MinMaxRuleDef}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{max, min}

import scala.collection.mutable.ArrayBuffer

/**
 * A grouping of rules to be applied to a dataframe or grouped dataframe. The input will never be grouped
 * but if the _groupBys are populated it's assumed the rules herein are applied by group
 */

class RuleSet extends SparkSessionWrapper {

  private val logger: Logger = Logger.getLogger(this.getClass)

  import spark.implicits._

  private var _df: DataFrame = _
  private var _isGrouped: Boolean = false
  private var _groupBys: Seq[String] = Seq.empty[String]
  private val _rules = ArrayBuffer[Rule]()

  private def setDF(value: DataFrame): this.type = {
    _df = value
    this
  }

  private def setIsGrouped(value: Boolean): this.type = {
    _isGrouped = value
    this
  }

  private def setGroupByCols(value: Seq[String]): this.type = {
    _groupBys = value
    _isGrouped = true
    this
  }

  private[validation] def getDf: DataFrame = _df

  private[validation] def isGrouped: Boolean = _isGrouped

  private[validation] def getGroupBys: Seq[String] = _groupBys

  def getRules: Array[Rule] = _rules.toArray

  /**
   * Generates two rules for each minmax definition one for the lower and one for the upper
   * Only valid for Bounds rule types
   * @param minMaxRuleDefs One or many minmax definitions as defined in Structures
   *                       Defined as case class to ensure proper usage
   * @return Array[Rule] that can be added to the RuleSet
   */
  def addMinMaxRules(minMaxRuleDefs: MinMaxRuleDef*): this.type = {

    add(minMaxRuleDefs.flatMap(ruleDef => {
      Seq(
        Rule(s"${ruleDef.ruleName}_min", min(ruleDef.column), ruleDef.bounds),
        Rule(s"${ruleDef.ruleName}_max", max(ruleDef.column), ruleDef.bounds)
      )
    }).toArray)
    this
  }

  /**
   * Builder pattern used to add individual MinMax rule sets after the RuleSet has been instantiated
   * @param ruleName name of rule
   * @param inputColumn input column (base or calculated)
   * @param boundaries lower/upper boundaries as defined by Bounds
   * @param by groupBy cols
   * @return RuleSet
   */
  def addMinMaxRules(ruleName: String,
                     inputColumn: Column,
                     boundaries: Bounds,
                     by: Column*
                    ): this.type = {
    val rules = Array(
      Rule(s"${ruleName}_min", min(inputColumn), boundaries),
      Rule(s"${ruleName}_max", max(inputColumn), boundaries)
    )
    add(rules)
  }

  /**
   * add array of rules
   * @param rules Array for Rules
   * @return RuleSet
   */
  def add(rules: Seq[Rule]): this.type = {
    rules.foreach(rule => _rules.append(rule))
    this
  }

  /**
   * Add a single rule
   * @param rule single defined rule
   * @return RuleSet
   */
  def add(rule: Rule): this.type = {
    _rules.append(rule)
    this
  }

  /**
   * Merge two rule sets by adding one rule set to another
   * @param ruleSet RuleSet to be added
   * @return RuleSet
   */
  def add(ruleSet: RuleSet): RuleSet = {
    new RuleSet().setDF(ruleSet.getDf)
      .setIsGrouped(ruleSet.isGrouped)
      .add(ruleSet.getRules)
  }

  /**
   * Logic to test compliance with provided rules added through the builder
   * TODO What else?
   * @return this but is marked private
   */
  private def validateRules(): Unit = {
    require(getRules.map(_.ruleName).distinct.length == getRules.map(_.ruleName).length,
      s"Duplicate Rule Names: ${getRules.map(_.ruleName).diff(getRules.map(_.ruleName).distinct).mkString(", ")}")
  }

  /**
   * Call the action once all rules have been applied
   * @param detailLevel -- For Future -- Perhaps faster way to just return true/false without
   *                    processing everything and returning a report. For big data sets, perhaps run samples
   *                    looking for invalids? Not sure how much faster and/or what the break-even would be
   * @return Tuple of Dataframe report and final boolean of whether all rules were passed
   */
  def validate(detailLevel: Int = 1): (DataFrame, Boolean) = {
    validateRules()
    Validator(this, detailLevel).validate
  }

}

object RuleSet {

  /**
   * Accepts DataFrame - Rules can be calculated for grouped DFs or non-grouped but not at the same time.
   * Either append rule[s] at call or via builder pattern
   */

  def apply(df: DataFrame): RuleSet = {
    new RuleSet().setDF(df)
  }

  def apply(df: DataFrame, by: Array[String]): RuleSet = {
    new RuleSet().setDF(df)
      .setGroupByCols(by)
  }

  def apply(df: DataFrame, by: String): RuleSet = {
    new RuleSet().setDF(df)
      .setGroupByCols(Array(by))
  }

  def apply(df: DataFrame, rules: Seq[Rule], by: Seq[String] = Seq.empty[String]): RuleSet = {
    new RuleSet().setDF(df)
      .setGroupByCols(by)
      .add(rules)
  }

  def apply(df: DataFrame, rules: Rule*): RuleSet = {
    new RuleSet().setDF(df)
      .add(rules)
  }

  /**
   * Generates two rules for each minmax definition one for the lower and one for the upper
   * Only valid for Bounds rule types
   * @param minMaxRuleDefs One or many minmax definitions as defined in Structures
   *                       Defined as case class to ensure proper usage
   * @return Array[Rule] that can be added to the RuleSet
   */
  def generateMinMaxRules(minMaxRuleDefs: MinMaxRuleDef*): Array[Rule] = {

    minMaxRuleDefs.flatMap(ruleDef => {
      Seq(
        Rule(s"${ruleDef.ruleName}_min", min(ruleDef.column), ruleDef.bounds),
        Rule(s"${ruleDef.ruleName}_max", max(ruleDef.column), ruleDef.bounds)
      )
    }).toArray
  }

}