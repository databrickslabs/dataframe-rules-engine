// Databricks notebook source
import com.databricks.labs.validation.utils.Structures._
import com.databricks.labs.validation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

// COMMAND ----------

// MAGIC %md
// MAGIC # Sample Dataset

// COMMAND ----------

object Lookups {
  final val validStoreIDs = Array(1001, 1002)
  final val validRegions = Array("Northeast", "Southeast", "Midwest", "Northwest", "Southcentral", "Southwest")
  final val validSkus = Array(123456, 122987, 123256, 173544, 163212, 365423, 168212)
  final val invalidSkus = Array(9123456, 9122987, 9123256, 9173544, 9163212, 9365423, 9168212)
}

val df = sc.parallelize(Seq(
    ("Northwest", 1001, 123456, 9.32, 8.99, 4.23, "2020-02-01 00:00:00.000"),
    ("Northwest", 1001, 123256, 19.99, 16.49, 12.99, "2020-02-01"),
    ("Northwest", 1001, 123456, 0.99, 0.99, 0.10, "2020-02-01"),
    ("Northwest", 1001, 123456, 0.98, 0.90, 0.10, "2020-02-01"), // non_distinct sku
    ("Northwst", 1001, 123456, 0.99, 0.99, 0.10, "2020-02-01"), // Misspelled Region
    ("Northwest", 1002, 122987, 9.99, 9.49, 6.49, "2021-02-01"), // Invalid Date/Timestamp
    ("Northwest", 1002, 173544, 1.29, 0.99, 1.23, "2020-02-01"),
    ("Northwest", 1002, 168212, 3.29, 1.99, 1.23, "2020-02-01"),
    ("Northwest", 1002, 365423, 1.29, 0.99, 1.23, "2020-02-01"),
    ("Northwest", 1002, 3897615, 14.99, 129.99, 1.23, "2020-02-01"),
    ("Northwest", 1003, 163212, 3.29, 1.99, 1.23, "2020-02-01") // Invalid numeric store_id groupby test
  )).toDF("region", "store_id", "sku", "retail_price", "scan_price", "cost", "create_ts")
    .withColumn("create_ts", 'create_ts.cast("timestamp"))
    .withColumn("create_dt", 'create_ts.cast("date"))

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC # Rule Types
// MAGIC There are several Rule types available:
// MAGIC 
// MAGIC 1. Categorical (numerical and string) - used to validate if row values fall in a pre-defined list of values, e.g. lookups
// MAGIC 2. Boundaries - used to validate if row values fall within a range of numerical values
// MAGIC 3. Expressions - used to validate if row values pass expressed conditions. These can be simple expressions like a Boolean column `col('valid')`, or complex, like `col('a') - col('b') > 0.0`

// COMMAND ----------

// MAGIC %md
// MAGIC ### Example 1: Writing your first Rule
// MAGIC Let's look at a very simple example...

// COMMAND ----------

// First, begin by defining your RuleSet by passing in your input DataFrame
val myRuleSet = RuleSet(df)

// Next, define a Rule that validates that the `store_id` values fall within a list of pre-defined Store Ids
val validStoreIdsRule = Rule("Valid_Store_Ids_Rule", col("store_id"), Array(1001, 1002))

// Finally, add the Rule to the RuleSet and validate!
val validationResults = myRuleSet.add(validStoreIdsRule).validate()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Viewing the Validation Results
// MAGIC 
// MAGIC The result from calling `validate()` on your RuleSet will be 2 DataFrames - a complete report and a summary report.
// MAGIC 
// MAGIC #### The completeReport
// MAGIC The complete report is verbose and will add all rule validations to the right side of the original df 
// MAGIC passed into RuleSet. Note that if the RuleSet is grouped, the result will include the groupBy columns and all rule
// MAGIC evaluation specs and results
// MAGIC 
// MAGIC #### The summaryReport
// MAGIC The summary report is meant to be just that, a summary of the failed rules. This will return only the records that 
// MAGIC failed and only the rules that failed for that record; thus, if the `summaryReport.isEmpty` then all rules passed.

// COMMAND ----------

// Let's look at the completeReport from the example above
display(validationResults.completeReport)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Example 2: Boundaries
// MAGIC Boundary Rules can be used to validate if row values fall within a range of numerical values.
// MAGIC 
// MAGIC It's quite common to generate many min/max boundaries and can be passed as an Array of Rules.

// COMMAND ----------

// Let's define several Boundary Rules to apply
val minMaxPriceDefs = Array(
  MinMaxRuleDef("MinMax_Sku_Price", col("retail_price"), Bounds(0.0, 29.99)),
  MinMaxRuleDef("MinMax_Scan_Price", col("scan_price"), Bounds(0.0, 29.99)),
  MinMaxRuleDef("MinMax_Cost", col("cost"), Bounds(0.0, 12.0))
)

// Add all the Rules at once using the array of Rules
val minMaxPriceRules = RuleSet(df).addMinMaxRules(minMaxPriceDefs: _*)

// Validate rows against all the Boundary Rules
val validationResults = minMaxPriceRules.validate()

// Let's look at the failed rows this time
display(validationResults.summaryReport)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Example 3: Expressions
// MAGIC Expressions can used to validate if row values pass expressed conditions. 
// MAGIC 
// MAGIC These can be simple expressions like a Boolean column `col('valid')`, or complex, like `col('a') - col('b') > 0.0`

// COMMAND ----------

// Ensure that each product has a distinct Product SKU
val distinctProductsRule = Rule("Unique_Skus", countDistinct("sku"), Bounds(upper = 1.0))
                              
// Rules can even be used in conjunction with user defined functions
def getDiscountPercentage(retailPrice: Column, scanPrice: Column): Column = {
  (retailPrice - scanPrice) / retailPrice
}
                                
val maxDiscountRule = Rule("Max_allowed_discount",
    max(getDiscountPercentage(col("retail_price"), col("scan_price"))),
    Bounds(upper = 90.0))
                                
// Notice the builder patthern. The idea is to buld up your rules and then add them to your RuleSet[s].
// RuleSets can be combined to using the RuleSet.add(ruleSet: RuleSet) method
var productRuleSet = RuleSet(df).add(distinctProductsRule)
                                .add(maxDiscountRule)
                                
// ...or add Rules together as an Array
val specializedProductRules = Array(distinctProductsRule, maxDiscountRule)
productRuleSet = RuleSet(df).add(specializedProductRules: _*)
                                
val validationResults = productRuleSet.validate()

display(validationResults.summaryReport)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inverting matches
// MAGIC We can even invert the match to validate row values do not fall in a list of values

// COMMAND ----------

// Invert match to ensure values are **not** in a LOV
val invalidStoreIdsRule = Rule("Invalid_Store_Ids_Rule", col("store_id"), Array(9001, 9002, 9003), invertMatch = true)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Case-sensitivity
// MAGIC Case-sensitivity is enabled by default. However, an optional `ignoreCase` parameter can be used to apply/not apply case sensitivity to a list of String values

// COMMAND ----------

// Numerical categorical rules. Build create a list of values to be validated against.
val catNumerics = Array(
  // Only allow store_ids in my validStoreIDs lookup
  Rule("Valid_Stores", col("store_id"), Lookups.validStoreIDs),
  // Validate against a pre-built list of skus that have been verified to be accurate
  // Currently this is manually created for demo but can easily be created from a dataframe, etc.
  Rule("Valid_Skus", col("sku"), Lookups.validSkus),
  // Ensure that the skus do not match any of the invalid skus defined earlier
  Rule("Invalid_Skus", col("sku"), Lookups.invalidSkus, invertMatch=true)
)

// Validate strings as well as numericals. They don't need to be in a separate array, it's just done here for demonstration
val catStrings = Array(
  // Case-sensitivity is enabled by default. However, `ignoreCase` parameter can be used 
  // to apply/not apply case sensitivity to a list of String values
  Rule("Valid_Regions", col("region"), Lookups.validRegions, ignoreCase=true)
)

// COMMAND ----------

// MAGIC %md
// MAGIC # Aggregates
// MAGIC Dataframes can be simple or a Seq of columns can be passed in as "bys" for the DataFrame to be grouped by. <br>
// MAGIC If the dataframe is grouped validations will be per group

// COMMAND ----------

// Grouped Dataframe
// Let's assume we want to perform validation by some grouping of one or many columns
val validationResults = RuleSet(df, Array("store_id"))
  .add(specializedProductRules)
  .add(minMaxPriceRules)
  .add(catNumerics)
  .add(catStrings)
  .validate()

display(validationResults.summaryReport)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Streaming DataFrames
// MAGIC Rules can be applied to streaming DataFrames, as well.

// COMMAND ----------

val yellowTaxi = spark.readStream
                      .format("delta")
                      .option("maxBytesPerTrigger", (1024 * 1024 * 4).toString)
                      .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")

// COMMAND ----------

val validPaymentTypes = Array("Cash", "Credit")
val rangeRules = Array(
  MinMaxRuleDef("Pickup Longitude On Earth", 'pickup_longitude, Bounds(-180, 180)),
  MinMaxRuleDef("Dropoff Longitude On Earth", 'dropoff_longitude, Bounds(-180, 180)),
  MinMaxRuleDef("Pickup Latitude On Earth", 'pickup_latitude, Bounds(-90, 90)),
  MinMaxRuleDef("Dropoff Latitude On Earth", 'dropoff_latitude, Bounds(-90, 90)),
  MinMaxRuleDef("Realistic Passenger Count", 'passenger_count, Bounds(1, 10))
)

val taxiBaseRules = Array(
  Rule("dropoff after pickup", (unix_timestamp('dropoff_datetime) * 1.05).cast("long") >= unix_timestamp('pickup_datetime)),
  Rule("total is sum of parts", 'fare_amount + 'extra + 'mta_tax + 'tip_amount + 'tolls_amount, 'total_amount),
  Rule("total greater than 0", 'total_amount > 0),
  Rule("valid payment types", lower('payment_type), validPaymentTypes)
)

val yellowTaxiReport = RuleSet(yellowTaxi)
  .add(taxiBaseRules: _*)
  .addMinMaxRules(rangeRules: _*)
  .validate()

// COMMAND ----------

display(
  yellowTaxiReport.summaryReport
)

// COMMAND ----------


