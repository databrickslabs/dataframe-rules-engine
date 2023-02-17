import unittest
import pyspark.sql
import pyspark.sql.functions as F

from databricks.labs.validation.rule import Rule
from databricks.labs.validation.rule_type import RuleType
from databricks.labs.validation.test.local_spark_singleton import SparkSingleton


class TestRule(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSingleton.get_instance()

    def test_string_lov(self):
        """Tests that a list of String values rule can be instantiated correctly."""

        # Ensure that a rule with a numerical LOV can be created
        building_name_rule = Rule("Building_LOV_Rule", column=F.col("site_name"),
                                valid_strings=["SiteA", "SiteB", "SiteC"])

        # Ensure that all attributes are set correctly for Integers
        assert (building_name_rule.rule_name() == "Building_LOV_Rule", "Rule name is not set as expected.")
        assert (building_name_rule.input_column_name() == "site_name", "Input column name is not set as expected.")
        assert(building_name_rule.rule_type() == RuleType.ValidateStrings, "The rule type is not set as expected.")
        assert(not building_name_rule.is_implicit_bool(), "The rule should not be an implicit boolean expression.")
        assert(not building_name_rule.is_agg(), "The rule should not be an aggregation.")

    def test_implicit_boolean(self):
        """Tests that an implicit boolean expression should be instantiated correctly."""

        # Ensure a single column of type boolean can be instantiated correctly
        cooling_bool_rule = Rule("Implicit_Cooling_Rule", F.col("cooling_bool"))

        # Ensure that all attributes are set correctly
        assert(cooling_bool_rule.rule_name() == "Implicit_Cooling_Rule", "Rule name is not set as expected.")
        assert(cooling_bool_rule.input_column_name() == "cooling_bool", "Input column name is not set as expected.")
        assert(cooling_bool_rule.rule_type() == RuleType.ValidateExpr, "The rule type is not set as expected.")
        assert(cooling_bool_rule.is_implicit_bool(), "The rule should not be an implicit boolean expression.")
        assert(not cooling_bool_rule.is_agg(), "The rule should not be an aggregation.")

        # Ensure that a boolean expression can be used to create an implicit boolean rule
        cooling_expr_rule = Rule("Implicit_Cooling_Expr", F.col("current_temp") > F.col("target_temp"))

        # Ensure that all attributes are set correctly
        assert(cooling_expr_rule.rule_name() == "Implicit_Cooling_Expr", "Rule name is not set as expected.")
        assert(cooling_expr_rule.input_column_name() == "(current_temp > target_temp)", "Input column name is not set as expected.")
        assert(cooling_expr_rule.rule_type() == RuleType.ValidateExpr, "The rule type is not set as expected.")
        assert(cooling_expr_rule.is_implicit_bool(), "The rule should not be an implicit boolean expression.")
        assert(not cooling_expr_rule.is_agg(), "The rule should not be an aggregation.")

    def test_column_expression(self):
        """ Tests that a column can be ruled equivalent to an expression."""

        # Ensure that equivalent comparision can be made between a column and expression
        cooling_bool_rule = Rule("Thermostat_Cooling_Rule", F.col("cooling_bool"),
                                  valid_expr=((F.col("current_temp") - F.col("target_temp")) >= 7.0))

        # Ensure that all attributes are set correctly
        assert(cooling_bool_rule.rule_name() == "Thermostat_Cooling_Rule", "Rule name is not set as expected.")
        assert(cooling_bool_rule.input_column_name() == "cooling_bool", "Input column name is not set as expected.")
        assert(cooling_bool_rule.rule_type() == RuleType.ValidateExpr, "The rule type is not set as expected.")
        assert(not cooling_bool_rule.is_implicit_bool(), "The rule should not be an implicit boolean expression.")
        assert(not cooling_bool_rule.is_agg(), "The rule should not be an aggregation.")

    def test_user_defined_functions(self):

        # Define a UDF that validates a timestamp column
        def valid_timestamp(ts_column: pyspark.sql.Column):
            return ts_column.isNotNull() & F.year(ts_column).isNotNull() & F.month(ts_column).isNotNull()

        valid_timestamp_rule = Rule("Valid_Timestamp_Rule", valid_timestamp(F.col("event_ts")))

        # Ensure that all attributes are set correctly
        assert(valid_timestamp_rule.rule_name() == "Valid_Timestamp_Rule", "Rule name is not set as expected.")
        assert(valid_timestamp_rule.input_column_name() == "valid_timestamp(F.col(event_ts))", "Input column name is not set as expected.")
        assert(valid_timestamp_rule.rule_type() == RuleType.ValidateExpr, "The rule type is not set as expected.")
        assert(not valid_timestamp_rule.is_implicit_bool(), "The rule should not be an implicit boolean expression.")
        assert(not valid_timestamp_rule.is_agg(), "The rule should not be an aggregation.")

    def tearDown(self):
        self.spark.stop()
