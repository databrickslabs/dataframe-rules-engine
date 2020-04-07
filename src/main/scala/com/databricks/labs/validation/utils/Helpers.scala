package com.databricks.labs.validation.utils

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.{col, explode, array, struct, lit}

object Helpers extends SparkSessionWrapper {

  import spark.implicits._
  private[validation] def getColumnName(c: Column): String = {
    try {
      c.expr.asInstanceOf[NamedExpression].name
    } catch {
      case e: ClassCastException => c.expr.references.map(_.name).toArray.head
    }
  }

}
