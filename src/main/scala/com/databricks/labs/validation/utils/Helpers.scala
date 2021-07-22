package com.databricks.labs.validation.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.NamedExpression

object Helpers extends SparkSessionWrapper {
  private[validation] def getColumnName(c: Column): String = {
    try {
      c.expr.asInstanceOf[NamedExpression].name
    } catch {
      case e: ClassCastException => c.expr.references.map(_.name).toArray.head
    }
  }

}
