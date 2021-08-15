package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.ValidationResults
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class Validator(ruleSet: RuleSet, detailLvl: Int) extends SparkSessionWrapper {

  private val byCols = ruleSet.getGroupBys map col

  private def buildFailureReport(df: DataFrame): DataFrame = {
    val rulesResultCols = ruleSet.getRules.map(r => s"`${r.ruleName}`").mkString(", ")
    val onlyFailedRecords = expr(s"""filter(array($rulesResultCols), results -> !results.passed)""")
    df.withColumn("failed_rules", onlyFailedRecords)
      .drop(ruleSet.getRules.map(_.ruleName): _*)
      .filter(size(col("failed_rules")) > 0)
  }

  private def evaluatedRules(rules: Array[Rule]): Array[Column] = {
    rules.map(rule => {
      rule.ruleType match {
        case RuleType.ValidateBounds =>
          struct(
            lit(rule.ruleName).alias("ruleName"),
            rule.boundaries.validationLogic(rule.inputColumn).alias("passed"),
            array(lit(rule.boundaries.lower), lit(rule.boundaries.upper)).cast("string").alias("permitted"),
            rule.inputColumn.cast("string").alias("actual")
          ).alias(rule.ruleName)
        case RuleType.ValidateNumerics =>
          val ruleExpr = if(rule.invertMatch) not(array_contains(rule.validNumerics, rule.inputColumn)) else array_contains(rule.validNumerics, rule.inputColumn)
          struct(
            lit(rule.ruleName).alias("ruleName"),
            ruleExpr.alias("passed"),
            rule.validNumerics.cast("string").alias("permitted"),
            rule.inputColumn.cast("string").alias("actual")
          ).alias(rule.ruleName)
        case RuleType.ValidateStrings =>
          val ruleValue = if(rule.ignoreCase) lower(rule.inputColumn) else rule.inputColumn
          val ruleExpr = if(rule.invertMatch) not(array_contains(rule.validStrings, ruleValue)) else array_contains(rule.validStrings, ruleValue)
          struct(
            lit(rule.ruleName).alias("ruleName"),
            ruleExpr.alias("passed"),
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