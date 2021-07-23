package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.ValidationResults
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class Validator(ruleSet: RuleSet, detailLvl: Int) extends SparkSessionWrapper {

  private val byCols = ruleSet.getGroupBys map col

  private def buildFailureReport(df: DataFrame): DataFrame = {
    val rulesResultsArray = array(ruleSet.getRules.map(_.ruleName) map col: _*)
    val onlyFailedRecords = expr(s"""filter($rulesResultsArray, results -> !results.passed)""")
    df.withColumn("failed_rules", onlyFailedRecords)
      .drop(ruleSet.getRules.map(_.ruleName): _*)
  }

  private def evaluatedRules(rules: Array[Rule]): Array[Column] = {
    rules.map(rule => {
      rule.ruleType match {
        case RuleType.ValidateBounds =>
          struct(
            lit(rule.ruleName).alias("ruleName"),
            (rule.inputColumn > rule.boundaries.lower && rule.inputColumn < rule.boundaries.upper)
              .alias("passed"),
            array(lit(rule.boundaries.lower), lit(rule.boundaries.upper)).cast("string").alias("permitted"),
            rule.inputColumn.cast("string").alias("actual")
          ).alias(rule.ruleName)
        case RuleType.ValidateNumerics =>
          struct(
            lit(rule.ruleName).alias("ruleName"),
            array_contains(rule.validNumerics, rule.inputColumn).alias("passed"),
            rule.validNumerics.cast("string").alias("permitted"),
            rule.inputColumn.cast("string").alias("actual")
          ).alias(rule.ruleName)
        case RuleType.ValidateStrings =>
          struct(
            lit(rule.ruleName).alias("ruleName"),
            array_contains(rule.validStrings, rule.inputColumn).alias("passed"),
            rule.validStrings.cast("string").alias("permitted"),
            rule.inputColumn.cast("string").alias("actual")
          ).alias(rule.ruleName)
        case RuleType.ValidateExpr =>
          struct(
            lit(rule.ruleName).alias("ruleName"),
            (rule.inputColumn === rule.validExpr).alias("passed"),
            lit(rule.inputColumnName).alias("permitted"),
            rule.inputColumn.cast("string").alias("actual")
          ).alias(rule.ruleName)
      }
    })
  }

  private[validation] def validate: ValidationResults = {

    val selects = evaluatedRules(ruleSet.getRules)

    val evaluatedDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select((ruleSet.getDf.columns map col) ++ selects: _*)
    } else {
      ruleSet.getDf
        .groupBy(byCols: _*)
        .agg(evaluatedRules(ruleSet.getRules).head, evaluatedRules(ruleSet.getRules).tail: _*)
        .select(byCols ++ (ruleSet.getRules.map(_.ruleName) map col): _*)
    }

    ValidationResults(evaluatedDF, buildFailureReport(evaluatedDF))
  }

}

object Validator {
  def apply(ruleSet: RuleSet, detailLvl: Int): Validator = new Validator(ruleSet, detailLvl)
}