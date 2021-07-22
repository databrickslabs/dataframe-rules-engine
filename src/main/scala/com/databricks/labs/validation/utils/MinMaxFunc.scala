package com.databricks.labs.validation.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.{array, max, min}

private[validation] class MinMaxFunc(e: Expression) extends Column(e)

object MinMaxFunc {

  private def apply(e: Expression): MinMaxFunc = new MinMaxFunc(e)

  def minMax(column: Column): MinMaxFunc = {
    val alias = s"${column}_labs_validation_MinMax"
    MinMaxFunc(array(min(column), max(column)).alias(alias).expr)
  }

}
