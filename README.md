[![Scala CI](https://github.com/databrickslabs/dataframe-rules-engine/actions/workflows/scala.yml/badge.svg?branch=master)](https://github.com/databrickslabs/dataframe-rules-engine/actions/workflows/scala.yml)
[![codecov](https://codecov.io/gh/databrickslabs/dataframe-rules-engine/branch/master/graph/badge.svg?token=6DEXO6I0BG)](https://codecov.io/gh/databrickslabs/dataframe-rules-engine)
# dataframe-rules-engine
Simplified Validation at scale for Production Spark Workloads on streaming / standard DataFrames and DataSets

## Project Description
As pipelines move from bronze to gold, it's very common that some level of governance be performed in
Silver or at various places in the pipeline. The need for business rule validation is very common.
Databricks recognizes this and, as such, is building Delta Pipelines with Expectations. 
Upon release of Delta Pipelines, the need for this package will be re-evaluated and the code base will 
be adjusted appropriately. This is serves an immediate need and Delta Expectations is expected to be a more, 
full-fledged and robust example of this functionality.

Introducing Databricks Labs - dataframe-rules-engine, a simple solution for validating data in dataframes before you
move the data to production and/or in-line (coming soon). 

![Alt Text](images/Rules_arch.png)
## Using The Rules Engine In Your Project
* Pull the latest release from the releases
* Add it as a dependency (will be in Maven eventually)
* Reference it in your imports

## Getting Started
Add [the dependency](https://mvnrepository.com/artifact/com.databricks.labs/dataframe-rules-engine_2.12) to your build.sbt or pom.xml

`libraryDependencies += "com.databricks.labs" %% "dataframe-rules-engine" % "0.2.0"`

```
<dependency>
    <groupId>com.databricks.labs</groupId>
    <artifactId>dataframe-rules-engine_2.12</artifactId>
    <version>0.2.0</version>
</dependency>
```

A list of usage examples is available in the `demo` folder of this repo in [html](demo/Rules_Engine_Examples.html) 
and as a [Databricks Notebook DBC](demo/Rules_Engine_Examples.dbc).

The process is simple:
* Define Rules
* Build a RuleSet from your Dataframe using your Rules you built
```scala
import com.databricks.labs.validation.utils.Structures._
import com.databricks.labs.validation._
```

## Streaming Update
As of version 0.2 streaming dataframes are fully supported

## Quickstart
The basic steps to validating data with the rules engine are:
* create rules
* create ruleset
* validate

Below are some examples to demonstrate the basic process.

```scala
val myRules = ??? // Definition of my base rules
val myAggRules = ??? // Definition of my agg rules
val validationResults = RuleSet(df)
  .add(myRules)
  .validate()

// or for validation executed at a grouped level
val validationResults = RuleSet(df, by = "myGroup")
        .add(myAggRules)
        .validate()

// grouping across multiple columns
val validationResults = RuleSet(df, by = Array("myGroupA", "myGroupB"))
        .add(myAggRules)
        .validate()
```

## Rules

There are four primary rule types
* [Simple Rules](#simple-rule)
* [Boundary Rules](#boundary-rules)
* [Implicit Boolean rules](#implicit-boolean-rules)
* [Categorical Rule](#categorical-rules)

These rule types can be applied to:
* Streaming Datasets
* Stateful Streaming Datasets
* Grouped Datasets
  * Important distinction as the rules apply only within the range of the grouped keys when a grouped dataset
  is passed for testing

### Simple Rule
A rule with a name, a check column, and an allowed value
```scala
Rule("Require_specific_version", col("version"), lit(0.2))
Rule("Require_version>=0.2", col("version") >= 0.2, lit(true))
```

### Implicit Boolean Rules
These rules are the same as columnar expression based rules except they don't require the comparison against `lit(true)`.
A type validation is done on the column before validation begins to ensure that the resolved expression resolves to
a boolean type.
```scala
// Passes where result is true
Rule("Require_version>=0.2", col("version") >= 0.2)
Rule("Require_version>=0.2", col("myDFBooleanCol"))
```

Note that the following is true, conceptually, since the implicit boolean compares against an implicit true. This 
just means that when you're using simple rules that resolve to true or false, you don't have to state it explicitly.
```scala
Rule("Require_version>=0.2", col("version") >= 0.2, lit(true)) == Rule("Require_version>=0.2", col("version") >= 0.2) 
```

### Boundary Rules
* Boundary Rules

  **Example:** `Rule("Longitude Range", col("longitude"), Bounds(-180, 180, lowerInclusive = true, upperInclusive = true))`
  * Rules that fail if the input column is outside of the specified boundaries
    * `Bounds(lower = 0.0)` **FAILS** when value >= 0.0
  * Boundary rules are created when the validation is of type `Bounds()`.
  * The default Bounds() are `Bounds(lower: Double = Double.NegativeInfinity, upper: Double = Double.PositiveInfinity,
    lowerInclusive: Boolean = false, upperInclusive: Boolean = false)`;
    therefore, if a lower / upper is not specified, any numeric value will pass.
  * **Inclusive Vs Exclusive:** When `Bounds` are defined, the user can decide whether to make a Boundary
    inclusive or exclusive. The **default** is exclusive.
    * **Exclusive Example:** `Bounds(0.0, lowerInclusive = true)` **FAILS** when 0.0 > value
    * **Exclusive Example:** `Bounds(0.0, 10.0)` **FAILS** when 0.0 >= value <= 10.0
    * **Inclusive Example:** `Bounds(0.0, 10.0, lowerInclusive = true, upperInclusive = true)` **FAILS** when 0.0 > value < 10.0
    * **Mixed Inclusion Example:** `Bounds(0.0, 10.0, lowerInclusive = true)` **FAILS** when 0.0 > value <= 10.0
  * Grouped Boundaries often come in pairs, to minimize the amount of rules that must be manually created, helper logic
    was created to define [MinMax Rules](#minmax-rules)
* Categorical Rules (Strings and Numerical)
  * Rules that fail if the result of the input column is not in a list of values
* Expression Rules
  * Rules that fail if the input column != defined expression

#### Additional Boundary Rule Examples
**Non-grouped RuleSet** - Passes when the retail_price in a record is exclusive between the Bounds
```scala
// Passes when retail_price > 0.0 AND retail_price < 6.99
Rule("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 6.99))
// Passes when retail_price >= 0.0 AND retail_price <= 6.99
Rule("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 6.99, lowerInclusive = true, upperInclusive = true))
// Passes when retail_price > 0.0
Rule("Retail_Price_GT0", col("retail_price"), Bounds(lower = 0.0))
// Passes when retail_price >= 0.0
Rule("Retail_Price_GT0", col("retail_price"), Bounds(lower = 0.0, lowerInclusive = true))
```
**Grouped RuleSet** - Passes when the minimum value in the group is within (exclusive) the boundary
```scala
// max(retail_price) > 0.0
Rule("Retail_Price_Validation", col("retail_price"), Bounds(lower = 0.0))
// min(retail_price) > 0.0 && min(retail_price) < 1000.0 within the group
Rule("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 1000.0))
```

### Categorical Rules
There are two types of categorical rules which are used to validate against a pre-defined list of valid
values. As of 0.2 accepted categorical types are String, Double, Int, Long but any types outside of this can
be input as an array() column of any type so long as it can be evaluated against the input column.

```scala
val catNumerics = Array(
Rule("Valid_Stores", col("store_id"), Lookups.validStoreIDs),
Rule("Valid_Skus", col("sku"), Lookups.validSkus),
Rule("Valid_zips", array_contains(col("zips"), expr("x -> f(x)")), lit(true))
)

val catStrings = Array(
Rule("Valid_Regions", col("region"), Lookups.validRegions)
)
```

An optional `ignoreCase` parameter can be specified when evaluating against a list of String values to ignore or apply
case-sensitivity. By default, input columns will be evaluated against a list of Strings with case-sensitivity applied.
```scala
Rule("Valid_Regions", col("region"), Lookups.validRegions, ignoreCase=true)
```

Furthermore, the evaluation of categorical rules can be inverted by specifying `invertMatch=true` as a parameter.
This can be handy when defining a Rule that an input column cannot match list of invalid values. For example:
```scala
Rule("Invalid_Skus", col("sku"), Lookups.invalidSkus, invertMatch=true)
``` 

### MinMax Rules
This is not considered a rule type as it isn't actually a rule type but rather a helper that builds in-between
rules for you when validating grouped datasets with agg functions.

It's very common to build rules on a grouped dataset to validate some upper and lower boundary within a group
so there's a helper function to speed up this process.
It really only makes sense to use minmax when specifying both an upper and a lower bound on a grouped dataset as
otherwise it's magically handled for you and it doesn't make sense.

Using this method in the example below will only require three lines of code instead of the 6 if each rule were built manually.
The same inclusive / exclusive overrides are available here as defined above.
```scala
val minMaxPriceDefs = Array(
  MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
  MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99, upperInclusive = true)),
  MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
)

// Generate the array of Rules from the minmax generator
val minMaxPriceRules = RuleSet.generateMinMaxRules(minMaxPriceDefs: _*)
```
OR -- simply add the list of minmax rules or simple individual rule definitions
to an existing RuleSet (if not using builder pattern)
```scala
val someRuleSet = RuleSet(df, by = "region_id")
someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
someRuleSet.addMinMaxRules("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 6.99))
```

Without minMax
```scala
import com.databricks.labs.validation.RuleSet
val validationReport = RuleSet(df, by = "region_id")
  .add(Rule("Min_Sku_Price", min(col("retail_price")), Bounds(0.0)))
  .add(Rule("Max_Sku_Price", max(col("retail_price")), Bounds(29.99, upperInclusive = true)))
// PLUS 4 more rules.
//.add(Rule(...))
//.add(Rule(...))
//.add(Rule(...))
//.add(Rule(...))
```

## Lists of Rules
A list of rules can be created as an Array and added to a RuleSet to simplify Rule management. It's very common 
for more complex sets of rules to be rolled up and packaged by business group / region / etc. These are also 
commonly packaged into logical structures (like case classes) and unrolled later and then unpacked into the right 
rule sets. This is made easy through the ability to add lists of rules in various ways.
```scala
val specializedRules = Array(
  // Example of aggregate column
  Rule("Reasonable_sku_counts", count(col("sku")), Bounds(lower = 20.0, upper = 200.0)),
  // Example of calculated column from catalyst UDF def getDiscountPercentage(retailPrice: Column, scanPrice: Column): Column = ???
  Rule("Max_allowed_discount",
    max(getDiscountPercentage(col("retail_price"), col("scan_price"))),
    Bounds(upper = 90.0)),
  // Example distinct values rule
  Rule("Unique_Skus", countDistinct("sku"), Bounds(upper = 1.0))
)
RuleSet(df, by = "store").add(specializedRules)
```

## List of Rules as JSON
An array of list of rules can be initialised from an external file containing valid json
Pass the Json as String as following 
```scala
val jsonParserInstance = new JsonRuleParser()
val rulesArray = jsonParserInstance.parseRules(jsonString)
```
The Array of Rules can then be used to in your Rule Set
Note: Currently this interface does not support MinMaxRule initialisation but will
be added in the next iteration

Common Real World Example
```scala
case class GlobalRules(regionID: Int, bu: String, subOrg: String, rules: Array[Rule]*)
// a structure like this will be fed from all over the world with their own specific rules that can all be tested
// on the global source of truth
```

## Constructing the Check Column
So far, we've only discussed simple column references as the input column, but remember, a column is just an
expression and thus, the check column can actually be a check expression
* simple column references `col("my_column_name")`
* complex columns `col("Revenue") - col("Cost")`
* aggregates `min("ColumnName")`
  * It can be confusing to mix aggregate and non-aggregate aggregate input columns. It's generally better to create two Rule Sets
  * If any of the rules' input columns are aggregates and no groupBy columns are provided 
    into the RuleSet the dataframe will be grouped by all df columns.

## Grouped Datasets
Rules can be applied to simple DataFrames or grouped Dataframes. To use a grouped dataframe simply pass
your dataframe into the RuleSet and pass one or more columns in as `by` columns. This will apply the rule
at the group level which can be helpful at times. Any input column expressions passed into a RuleSet must be able
to be evaluated inside of the `.agg()` of a `groupedDataframe`
```scala
RuleSet(df, by = "region_id") 
// 
RuleSet(df, by = Seq("region_id", "store_id"))
```

Below shows a more, real-world example of validating a dataset and another way to instantiate a RuleSet.
```scala
def momValue(c: Column): Column = coalesce(lag(c, 1).over(regionalTimeW), c) / c

val regionalTimeW = Window.partitionBy(col("region_id")).orderBy(col("year"), col("month"))
val regionalRules = Array(
  // No region has more than 42 stores, thus 100 is a safe fat-finger check number
  Rule("storeCount", countDistinct(col("store_id")), Bounds(0, 100, inclusiveLower = true)),
  // month over month sales should be pretty stable within the region, if it's not, flag for review
  Rule("momSalesIncrease", momValue(col("total_sales")), Bounds(0.25, 4.0), inclusiveLower = true)
)
RuleSet(df, regionalRules, by = "region_id")
```


### Validation
Now that you have some rules built up... it's time to build the ruleset and validate it. As mentioned above,
the dataframe can be a simple df or a grouped df by passing column[s] to perform validation at the 
defined grouped level.

The below is meant as a theoretical example, it will not execute because rules containing aggregate input columns
AND non-aggregate input columns are defined throughout the rules added to the RuleSet. In practice if rules need to 
be validated at different levels, it's best to complete a validation at each level with a RuleSet at that level.
```scala
val validationResults = RuleSet(df)
.add(specializedRules)
.add(minMaxPriceRules)
.add(catNumerics)
.add(catStrings)
.validate()

val validationResults = RuleSet(df, Array("store_id"))
.add(specializedRules)
.add(minMaxPriceRules)
.add(catNumerics)
.add(catStrings)
.validate()
``` 

The `validate()` method returns a case class of ValidationResults which is defined as:
```scala
ValidationResults(completeReport: DataFrame, summaryReport: DataFrame)
```
AS you can see, there are two reports included, a `completeReport` and a `summaryReport`. 
#### The completeReport
`validationResults.completeReport.show()`

The complete report is verbose and will add all rule validations to the right side of the original df 
passed into RuleSet. Note that if the RuleSet is grouped, the result will include the groupBy columns and all rule
evaluation specs and results

#### The summaryReport
`validationResults.summaryReport.show()`

The summary report is meant to be just that, a summary of the failed rules. This will return only the records that 
failed and only the rules that failed for that record; thus, if the `summaryReport.isEmpty` then all rules passed.

## Next Steps
Clearly, this is just a start. This is a small package and, as such, a GREAT place to start if you've never
contributed to a project before. Please feel free to fork the repo and/or submit PRs. We'd love to see what
you come up with. If you're not much of a developer or don't have the time you can still contribute! Please
post your ideas in the issues and label them appropriately (i.e. bug/enhancement) and someone will review it 
and add it as soon as possible.

Some ideas of great adds are:
* Add a Python wrapper
* Enable an external table to host the rules and have rules compiled from externally managed source (GREAT idea from Sri Tikkireddy)
* Implement smart sampling for large datasets and faster validation

## Legal Information
This software is provided as-is and is not officially supported by Databricks through customer technical support channels.
Support, questions, and feature requests can be submitted through the Issues page of this repo.
Please see the [legal agreement](LICENSE.txt) and understand that issues with the use of this code will 
not be answered or investigated by Databricks Support.  

## Core Contribution team
* Lead Developer: [Daniel Tomes](https://www.linkedin.com/in/tomes/), Principal Architect, Databricks

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, 
and are not formally supported by Databricks with Service Level Agreements (SLAs).  
They are provided AS-IS and we do not make any guarantees of any kind.  
Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  
They will be reviewed as time permits, but there are no formal SLAs for support.


## Building the Project
To build the project: <br>
```
cd Downloads
git pull repo
sbt clean package
```

## Running tests
To run tests on the project: <br>
```
sbt test
```

Make sure that your JAVA_HOME is setup for sbt to run the tests properly. You will need JDK 8 as Spark does
not support newer versions of the JDK.

## Test reports for test coverage
To get test coverage report for the project: <br>
```
sbt jacoco
```

The test reports can be found in target/scala-<version>/jacoco/

