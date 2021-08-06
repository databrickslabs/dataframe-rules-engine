# dataframe-rules-engine
Simplified Validation for Production Workloads

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

As of version 0.2 There are three primary rule types
* Boundary Rules
  * Rules that fail if the input column is outside of the specified boundaries
  * Boundary Rules are **EXCLUSIVE** on both sides meaning Bounds(0.0, 10.0) will fail all values between and 
  including 0.0 and 10.0
  * Boundary rules are created when the validation is of type `Bounds()`.
    * The default Bounds() are `Bounds(lower: Double = Double.NegativeInfinity, upper: Double = Double.PositiveInfinity)`;
    therefore, if a lower or an upper is not specified, any value will pass
  * Group Boundaries often come in pairs, to minimize the amount of rules that must be manually created, helper logic 
  was created to define [MinMax Rules](#minmax-rules)
* Categorical Rules (Strings and Numerical)
  * Rules that fail if the result of the input column is not in a list of values
* Expression Rules
  * Rules that fail if the input column != defined expression 

Rules' input columns can be composed of: 
* simple column references `col("my_column_name")`
* complex columns `col("Revenue") - col("Cost")`
* implicit boolean evaluation `lit(true)`
  * These rules only take a single input column and it must resolve to true or false. All records in which it resolves 
  to true will be considered passed
* aggregate columns `min("ColumnName")`
  * Do not mix aggregate input columns with non-aggregate input columns, instead create two Rule Sets
  * If the rules' input columns are a mixture of aggregates and non-aggregates and no groupBy columns are passed 
  into the RuleSet the dataframe will be grouped by all df columns

Rules can be applied to simple DataFrames or grouped Dataframes. To use a grouped dataframe simply pass 
your dataframe into the RuleSet and pass one or more columns in as `by` columns. This will apply the rule
at the group level which can be helpful at times. Any input column expressions passed into a RuleSet must be able 
to be evaluated inside of the `.agg()` of a groupedDataframe

### Simple Rule
```scala
Rule("Require_specific_version", col("version"), lit(0.2))
Rule("Require_version>=0.2", col("version") >= 0.2, lit(true))
```

### Simple Boundary Rule
**Non-grouped RuleSet** - Passes when the retail_price in a record is exclusive between the Bounds
```scala
// Passes when retail_price > 0.0 AND retail_price < 6.99
Rule("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 6.99))
// Passes when retail_price > 0.0
Rule("Retail_Price_GT0", col("retail_price"), Bounds(lower = 0.0))
```
**Grouped RuleSet** - Passes when the minimum value in the group is within (exclusive) the boundary
```scala
// max(retail_price) > 0.0
Rule("Retail_Price_Validation", col("retail_price"), Bounds(lower = 0.0))
// min(retail_price) > 0.0 && min(retail_price) < 1000.0 within the group
Rule("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 1000.0))
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

### List of Rules
A list of rules can be created as an Array and passed into the RuleSet to simplify Rule management
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
          
  RuleSet(df, by = "store").add(specializedRules)
)
```

### MinMax Rules
It's very common to build rules to validate min and max allowable values so there's a helper function
to speed up this process. It really only makes sense to use minmax when specifying both an upper and a lower bound
in the Bounds object. Using this method in the example below will only require three lines of code instead of the 6
if each rule were built manually
```scala
val minMaxPriceDefs = Array(
  MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
  MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99)),
  MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
)

// Generate the array of Rules from the minmax generator
val minMaxPriceRules = RuleSet.generateMinMaxRules(minMaxPriceDefs: _*)
```
OR -- simply add the list of minmax rules or simple individual rule definitions
to an existing RuleSet (if not using builder pattern)
```scala
val someRuleSet = RuleSet(df)
someRuleSet.addMinMaxRules(minMaxPriceDefs: _*)
someRuleSet.addMinMaxRules("Retail_Price_Validation", col("retail_price"), Bounds(0.0, 6.99))
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
The complete report is verbose and will add all rule validations to the right side of the original df 
passed into RuleSet. Note that if the RuleSet is grouped, the result will include the groupBy columns and all rule
evaluation specs and results

#### The summaryReport
The summary report is meant to be just that, a summary of the failed rules. This will return only the records that 
failed and only the rules that failed for that record; thus, if the `summaryReport.isEmpty` then all rules passed.

## Next Steps
Clearly, this is just a start. This is a small package and, as such, a GREAT place to start if you've never
contributed to a project before. Please feel free to fork the repo and/or submit PRs. I'd love to see what
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

