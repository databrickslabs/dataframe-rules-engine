package com.databricks.labs.validation

import com.databricks.labs.validation.utils.Structures.Bounds
import com.databricks.labs.validation.{JsonRuleParser, Rule}
import org.apache.spark.sql.functions.{col, expr}
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class RuleParserTestSuite extends AnyFunSuite with SparkSessionFixture{

  test("Input json is Read Correctly with expected columns"){

    val jsonParserInstance = new JsonRuleParser()
    val parsedString=jsonParserInstance.readRules()
    println(parsedString)
    assert (parsedString.isInstanceOf[String])
    }




test("Json is parsed to Array of Rules"){

  val jsonParserInstance = new JsonRuleParser()
  val parsedString=jsonParserInstance.readRules()
  val rulesArray = jsonParserInstance.parseRules(parsedString)

  assert (rulesArray.isInstanceOf[Array[Rule]])
  assert(rulesArray.length===8)
}

  test("Json of Rules is initialised to expected rules"){
    val jsonParserInstance = new JsonRuleParser()
    val parsedString=jsonParserInstance.readRules()
    val rulesArray = jsonParserInstance.parseRules(parsedString)

    //Rule 1 - Implicit Boolean Rule

    val testRule1 = Rule("ImplicitCoolingExpr",col("booleanisithot"))
    print(rulesArray(0))
    print(testRule1)
    assert(rulesArray(0).ruleName===testRule1.ruleName)
    assert(rulesArray(0).inputColumnName===testRule1.inputColumnName)

    //Rule 2a - Rule with Bounds with lower and upper inclusive set

    val testRule2 = Rule("HeatingRateIntRulewith2Bounds",col("heatingrate-coolingrate"),Bounds(0.01, 1000.0,lowerInclusive=true,upperInclusive=true) )
    print(rulesArray(1))
    print(testRule2)
    assert(rulesArray(1).ruleName===testRule2.ruleName)
    assert(rulesArray(1).inputColumnName===testRule2.inputColumnName)
    assert(
      (rulesArray(1).boundaries.lower==testRule2.boundaries.lower) &&
      (rulesArray(1).boundaries.upper==testRule2.boundaries.upper) &&
      (rulesArray(1).boundaries.lowerInclusive==testRule2.boundaries.lowerInclusive) &&
      (rulesArray(1).boundaries.upperInclusive==testRule2.boundaries.upperInclusive),"Boundary is not set up correctly"
    )

    //Rule 2b - Rule with Bounds with lower inclusive set

    val testRule3 = Rule("HeatingRateIntRulewith1bounds",col("heatingrate-coolingrate"),Bounds(0.01, 1000.0,lowerInclusive=true) )
    print(rulesArray(2))
    print(testRule3)
    assert(rulesArray(2).ruleName===testRule3.ruleName,"Rule Name not set correctly")
    assert(rulesArray(2).inputColumnName===testRule3.inputColumnName)
    assert(
      (rulesArray(2).boundaries.lower==testRule3.boundaries.lower) &&
        (rulesArray(2).boundaries.upper==testRule3.boundaries.upper) &&
        (rulesArray(2).boundaries.lowerInclusive==testRule3.boundaries.lowerInclusive) &&
        (rulesArray(2).boundaries.upperInclusive==testRule3.boundaries.upperInclusive),"Boundary is not set up correctly"
    )


    val testRule4 = Rule("HeatingRateIntRulewithCategoryLookup",col("heatingrate-coolingrate"),Array("Good","Bad") )
    print(rulesArray(3))
    print(testRule4)
    assert(rulesArray(3).ruleName===testRule4.ruleName)
    assert(rulesArray(3).inputColumnName===testRule4.inputColumnName)
    assert(rulesArray(3).ruleType == RuleType.ValidateStrings,"Rule Type is not set up correctly")

    val testRule5 = Rule("HeatingRateIntRulewithCategoryLookupIgnorecase",col("heatingrate-coolingrate"),Array("Good","Bad"),ignoreCase = true )
    print(rulesArray(4))
    print(testRule5)
    assert(rulesArray(4).ruleName===testRule5.ruleName)
    assert(rulesArray(4).inputColumnName===testRule5.inputColumnName)
    assert(rulesArray(4).ruleType == RuleType.ValidateStrings,"Rule Type is not set up correctly")

    val testRule6 = Rule("HeatingRateIntRulewithCategoryLookupInt",col("heatingrate-coolingrate"),Array(1,10,100,1000))
    print(rulesArray(6))
    print(testRule6)
    assert(rulesArray(6).ruleName===testRule6.ruleName)
    assert(rulesArray(6).inputColumnName===testRule6.inputColumnName)
    assert(rulesArray(6).ruleType == RuleType.ValidateNumerics,"Rule Type is not set up correctly")


  }




}
