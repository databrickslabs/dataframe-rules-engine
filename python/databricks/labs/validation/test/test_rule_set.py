import unittest

from databricks.labs.validation.rule import Rule
from databricks.labs.validation.rule_set import RuleSet
from databricks.labs.validation.test.local_spark_singleton import SparkSingleton

import pyspark.sql.functions as F


class TestRuleSet(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSingleton.get_instance()

    def test_add_rules(self):
        iot_readings = [
            (1001, "zone_a", 50.1),
            (1002, "zone_b", 25.4),
            (1003, "zone_c", None)
        ]
        df = self.spark.createDataFrame(iot_readings).toDF("device_id", "zone_id", "temperature")
        rule_set = RuleSet(df)
        temp_rule = Rule("valid_temp", F.col("temperature").isNotNull())
        rule_set.add(temp_rule)

        # Ensure that the RuleSet DF can be set/gotten correctly
        rule_set_df = rule_set.get_df()
        assert(rule_set_df.count() == 3)
        assert(rule_set_df.columns.contains("device_id"))
        assert(rule_set_df.columns.contains("zone_id"))
        assert(rule_set_df.columns.contains("temperature"))

        # Ensure that the summary report contains failed rules
        validation_summary = rule_set.get_summary_report()
        assert(validation_summary.where(F.col("failed_rules").isNotNull()).count() == 1)

    def tearDown(self):
        self.spark.stop()
