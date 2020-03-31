package com.databricks.labs.validation.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.NamedExpression

object Helpers {

  private[validation] def getColumnName(c: Column): String = {
    c.expr.asInstanceOf[NamedExpression].name
  }

}
