package com.databricks.labs.validation

import io.circe.{Decoder, Json, parser}
import com.databricks.labs.validation.utils.Structures.Bounds
import org.apache.spark.sql.functions.col

import scala.io.Source




sealed trait RuleParser{
  /**
   * Define trait to enable extension of generic Rule Parser to provide a body support multiple input types
   * */

  val parserType:String
  /** Identifier to be implemented by child class specifying format it supports*/
  def readRules(filePath:String):String
  /** @param filePath The specific path where the json file containing the rules reside */
  def parseRules(rules:String):Array[Rule]
  /** @param rules input object of genric Type T containing the parsed rules
   * returns Array of Individual Rules specified in JSON object*/
}




class JsonRuleParser extends RuleParser {
  /**
   * Implementation of RuleParser to support external rules in JSON Format
   *
   *
   * */
  override final val parserType ="jsonParser"
  def parseRules(rules:String):Array[Rule] = {
    if (rules == null) {
      val jsonRules = parser.decode[Array[Rule]](rules).right.get
      jsonRules
    }
    else{
    val jsonRules = parser.decode[Array[Rule]](readRules()).right.get
    jsonRules
    }
  }

  def readRules(filePath:String="rules.json"):String = {
    val jsonRuleString: String = Source.fromResource(filePath).mkString
    jsonRuleString
  }

/**
 * Implicit decoder types needed by CIRCE lib to parse individual json items to be parsed to the supported Rule objects
 * */

  private val _boundsDecoder:Decoder[Bounds]={
    boundCursor =>
      for {
        lower <- boundCursor.get[Double]("lower")
        upper <- boundCursor.get[Double]("upper")
        lowerInclusive <- boundCursor.getOrElse[Boolean]("lowerInclusive")(false)
        upperInclusive <- boundCursor.getOrElse[Boolean]("upperInclusive")(false)
      } yield Bounds(lower , upper , lowerInclusive , upperInclusive )
  }

  private val _ruleDecoder1:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        bounds <- ruleCursor.get[Bounds]("Bounds")
      } yield Rule(ruleName, column ,bounds)

  }

  private val _ruleDecoder2:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
      } yield Rule(ruleName, column)
  }

  private val _ruleDecoder3:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validExpr <- ruleCursor.get[String]("validExpr").map(x => col(x))
      } yield Rule(ruleName , column ,validExpr)
  }

  private val _ruleDecoder4:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validNumerics <- ruleCursor.get[Array[Double]]("validNumerics")
        invertMatch <- ruleCursor.get[Boolean]("invertMatch")
      } yield Rule(ruleName , column ,validNumerics,invertMatch)
  }

  private val _ruleDecoder5a:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validNumerics <- ruleCursor.get[Array[Double]]("validNumerics")

      } yield Rule(ruleName , column ,validNumerics)

  }

  private val _ruleDecoder5b:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validNumerics <- ruleCursor.get[Array[Long]]("validNumerics")

      } yield Rule(ruleName , column ,validNumerics)

  }

  private val _ruleDecoder5c:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validNumerics <- ruleCursor.get[Array[Int]]("validNumerics")

      } yield Rule(ruleName , column ,validNumerics)

  }

  private val _ruleDecoder5d1:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validNumerics <- ruleCursor.get[Array[Double]]("validNumerics")
        invertMatch <- ruleCursor.getOrElse[Boolean]("invertMatch")(false)

      } yield Rule(ruleName , column ,validNumerics, invertMatch)

  }

  private val _ruleDecoder5d2:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validNumerics <- ruleCursor.get[Array[Int]]("validNumerics")
        invertMatch <- ruleCursor.getOrElse[Boolean]("invertMatch")(false)

      } yield Rule(ruleName , column ,validNumerics, invertMatch)

  }

  private val _ruleDecoder6:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validString <- ruleCursor.get[Array[String]]("validString")
        ignoreCase <- ruleCursor.getOrElse[Boolean]("ignoreCase")(false)
        invertMatch <- ruleCursor.getOrElse[Boolean]("invertMatch")(false)
      } yield Rule(ruleName , column ,validString,ignoreCase,invertMatch)
  }

  private val _ruleDecoder7:Decoder[Rule]={
    ruleCursor =>
      for {
        ruleName <- ruleCursor.get[String]("ruleName")
        column <- ruleCursor.get[String]("column").map(x => col(x))
        validString <- ruleCursor.get[Array[String]]("validString")
      } yield Rule(ruleName , column ,validString)
  }

  implicit val boundsDecoder: Decoder[Bounds] = _boundsDecoder
  implicit val ruleDecoder: Decoder[Rule] = {

      _ruleDecoder3 or
      _ruleDecoder4 or
      _ruleDecoder5a or
      _ruleDecoder5b or
      _ruleDecoder5c or
      _ruleDecoder5d1 or
      _ruleDecoder5d2 or
      _ruleDecoder6 or
      _ruleDecoder7 or
      _ruleDecoder1 or
      _ruleDecoder2
  }


}
