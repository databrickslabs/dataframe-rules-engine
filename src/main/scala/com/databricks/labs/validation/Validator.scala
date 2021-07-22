package com.databricks.labs.validation

import com.databricks.labs.validation.utils.SparkSessionWrapper
import com.databricks.labs.validation.utils.Structures.ValidationResults
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, col, collect_set, explode, expr, lit, struct, sum, when}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types._

import scala.collection.mutable

class Validator(ruleSet: RuleSet, detailLvl: Int) extends SparkSessionWrapper {

  import spark.implicits._

  private val boundaryRules = ruleSet.getRules.filter(_.ruleType == RuleType.ValidateBounds)
  private val categoricalRules = ruleSet.getRules.filter(rule => rule.ruleType == RuleType.ValidateNumerics ||
    rule.ruleType == RuleType.ValidateStrings)
  private val dateTimeRules = ruleSet.getRules.filter(_.ruleType == RuleType.ValidateDateTime)
  private val complexRules = ruleSet.getRules.filter(_.ruleType == RuleType.ValidateComplex)
  private val byCols = ruleSet.getGroupBys map col

  /**
   * Method for simplifying complex select statements that end up with an output
   * One or more select statements can be used for more complex, lengthy select sequences
   * @param output
   * @param select
   */
  case class Selects(output: Column, select: Column*)

  /**
   * Builds struct for validations by type
   * Initialize all structs to null and populate for the rule
   * @param rule Specific rule
   * @return struct of validation values containing the input validation values for the rule
   */
  private def buildValidationsByType(rule: Rule): Column = {
    val nulls = mutable.Map[String, Column](
      RuleType.ValidateBounds.toString -> lit(null).cast(ArrayType(DoubleType)).alias(RuleType.ValidateBounds.toString),
      RuleType.ValidateNumerics.toString -> lit(null).cast(ArrayType(DoubleType)).alias(RuleType.ValidateNumerics.toString),
      RuleType.ValidateStrings.toString -> lit(null).cast(ArrayType(StringType)).alias(RuleType.ValidateStrings.toString),
      RuleType.ValidateDateTime.toString -> lit(null).cast(LongType).alias(RuleType.ValidateDateTime.toString)
    )
    rule.ruleType match {
      case RuleType.ValidateBounds => nulls(RuleType.ValidateBounds.toString) = array(lit(rule.boundaries.lower), lit(rule.boundaries.upper)).alias(RuleType.ValidateBounds.toString)
      case RuleType.ValidateNumerics => nulls(RuleType.ValidateNumerics.toString) = lit(rule.validNumerics).alias(RuleType.ValidateNumerics.toString)
      case RuleType.ValidateStrings => nulls(RuleType.ValidateStrings.toString) = lit(rule.validStrings).alias(RuleType.ValidateStrings.toString)
    }
    val validationsByType = nulls.toMap.values.toSeq
    struct(
      validationsByType: _*
    ).alias("Validation_Values")
  }

  /**
   * Build the struct for the rule and the results
   * @param rule specific rule for struct to compile
   * @param results result of pass/fail and/or counts of failures
   * @return
   */
  private def buildOutputStruct(rule: Rule, results: Seq[Column]): Column = {
    struct(
      lit(rule.ruleName).alias("Rule_Name"),
      lit(rule.ruleType.toString).alias("Rule_Type"),
      buildValidationsByType(rule),
      struct(results: _*).alias("Results")
    ).alias("Validation")
  }

  /**
   * Create output from struct for simplified results report
   * @param df array of structs with results by Rule
   * @return
   */
  private def simplifyReport(df: DataFrame): DataFrame = {
    val summaryCols = Seq(
      col("Validations.Rule_Name"),
      col("Validations.Rule_Type"),
      col("Validations.Validation_Values"),
      col("Validations.Results.Invalid_Count"),
      col("Validations.Results.Failed")
    )
    if (ruleSet.getGroupBys.isEmpty) {
      df.select(summaryCols: _*)
        .orderBy('Failed.desc, 'Rule_Name)
    } else {
      df.select(byCols ++ summaryCols: _*)
        .orderBy('Failed.desc, 'Rule_Name)
    }
  }

  /**
   * Primary logic to calculate the threshold logic and build the select structs for output summary
   * @param rules input for rules within scope
   * @return
   */
  private def buildBaseSelects(rules: Array[Rule]): Array[Selects] = {

    // Build base selects
    rules.map(rule => {

      // Results must have Invalid_Count & Failed
      rule.ruleType match {
        case RuleType.ValidateBounds =>
          // Rule evaluation for NON-AGG RULES ONLY
          val invalid = rule.inputColumn < rule.boundaries.lower || rule.inputColumn > rule.boundaries.upper
          // This is the first select it must come before subsequent selects as it aliases the original column name
          // to that of the rule name. ADDITIONALLY, this evaluates the boundary rule WHEN the input col is not an Agg.
          // This can be confusing because for Non-agg columns it renames the column to the rule_name AND returns a 0
          // or 1 (not the original value)
          // IF the rule is NOT an AGG then the column is simply aliased to the rule name and no evaluation takes place
          // here.
          val first = if (!rule.isAgg) { // Not Agg
            sum(when(invalid, 1).otherwise(0)).alias(rule.ruleName)
          } else { // Is Agg
            rule.inputColumn.alias(rule.ruleName)
          }
          // WHEN RULE IS AGG -- this is where the evaluation happens. The input column was renamed to the name of the
          // rule in the required previous select.
          // IMPORTANT: REMEMBER - that agg expressions evaluate to a single output value thus the invalid_count in
          // cases where agg is used cannot be > 1 since the sum of a single value cannot exceed 1.

          // WHEN RULE NOT AGG - determine if the result of "first" select (0 or 1) is > 0, if it is, the rule has
          // failed since the sum(1 or more 1s) means that 1 or more rows have failed thus the rule has failed
          val failed = if (rule.isAgg) {
            when(
              col(rule.ruleName) < rule.boundaries.lower || col(rule.ruleName) > rule.boundaries.upper, true)
              .otherwise(false).alias("Failed")
          } else{
            when(col(rule.ruleName) > 0,true).otherwise(false).alias("Failed")
          }
          val results = if (rule.isAgg) {
            Seq(when(failed, 1).otherwise(0).cast(LongType).alias("Invalid_Count"), failed)
          } else {
            Seq(col(rule.ruleName).cast(LongType).alias("Invalid_Count"), failed)
          }
          Selects(buildOutputStruct(rule, results), first)
        case x if x == RuleType.ValidateNumerics || x == RuleType.ValidateStrings =>
          val invalid = if (x == RuleType.ValidateNumerics) {
            expr(s"size(array_except(${rule.ruleName}," +
              s"array(${rule.validNumerics.mkString("D,")}D)))")
          } else {
            expr(s"size(array_except(${rule.ruleName}," +
              s"array('${rule.validStrings.mkString("','")}')))")
          }
          val failed = when(invalid > 0, true).otherwise(false).alias("Failed")
          // TODO -- Cardinality check and WARNING
          val first = collect_set(rule.inputColumn).alias(rule.ruleName)
          val results = Seq(invalid.cast(LongType).alias("Invalid_Count"), failed)
          Selects(buildOutputStruct(rule, results), first)
        case RuleType.ValidateDateTime => ??? // TODO
        case RuleType.ValidateComplex => ??? // TODO
      }
    })
  }

  /**
   * TODO - Implement rule handlers for dates accepting
   * Column Type (i.e. current_timestamp and current_date)
   * java.util.Date
   * Validated strings (regex?) to pass into spark and convert to date/ts
   */
  private def validatedateTimeRules: Unit = ???

  /**
   * Are there common complex rule patterns that should be added?
   */
  private def validateComplexRules: Unit = ???

  private[validation] def validate: ValidationResults = {

    //    val selects = buildBaseSelects(boundaryRules)
    val selects = buildBaseSelects(boundaryRules) ++ buildBaseSelects(categoricalRules)
    val fullOutput = explode(array(selects.map(_.output): _*)).alias("Validations")
    val summaryDF = if (ruleSet.getGroupBys.isEmpty) {
      ruleSet.getDf
        .select(selects.map(_.select.head): _*)
        .select(fullOutput)
    } else {
      ruleSet.getDf
        .groupBy(byCols: _*)
        .agg(selects.map(_.select.head).head, selects.map(_.select.head).tail: _*)
        .select(byCols :+ fullOutput: _*)
    }

    val validationSummaryDF = simplifyReport(summaryDF)

    val passed: Option[Boolean] = if (!ruleSet.getDf.isStreaming) {
      println("SOURCE IS NOT STREAMING")
      Some(validationSummaryDF.filter('Failed === true).count == 0)
    } else None
    println("SOURCE IS STREAMING")
    ValidationResults(validationSummaryDF, None, passed)
  }

}

object Validator {
  def apply(ruleSet: RuleSet, detailLvl: Int): Validator = new Validator(ruleSet, detailLvl)
}