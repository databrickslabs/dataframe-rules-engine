import pyspark
from typing import List

from databricks.labs.validation.local_spark_singleton import SparkSingleton
from databricks.labs.validation.rule_type import RuleType
from databricks.labs.validation.structures import Bounds
from databricks.labs.validation.utils.helpers import Helpers


class Rule:
    """
    Definition of a rule
    """

    # TODO: Fix type hint for valid_numerics
    def __init__(self,
                 rule_name: str,
                 column: pyspark.sql.Column,
                 boundaries: Bounds = None,
                 valid_expr: pyspark.sql.Column = None,
                 valid_strings: List[str] = None,
                 valid_numerics=None,
                 ignore_case: bool = False,
                 invert_match: bool = False):

        self._spark = SparkSingleton.get_instance()
        self._column = column
        self._boundaries = boundaries
        self._valid_expr = valid_expr
        self._valid_strings = valid_strings
        self._valid_numerics = valid_numerics
        self._is_implicit_bool = False

        # Determine the Rule type by parsing the input arguments
        if valid_strings is not None and len(valid_strings) > 0:
            j_valid_strings = Helpers.to_java_array(valid_strings, self._spark._sc)
            self._jRule = self._spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name, column._jc,
                                                                                     j_valid_strings,
                                                                                     ignore_case, invert_match)
            self._rule_type = RuleType.ValidateStrings

        elif valid_numerics is not None and len(valid_numerics) > 0:
            j_valid_numerics = Helpers.to_java_array(valid_numerics, self._spark._sc)
            self._jRule = self._spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name,
                                                                                     column._jc,
                                                                                     j_valid_numerics)
            self._rule_type = RuleType.ValidateNumerics
        else:
            self._jRule = self._spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name, column._jc)
            self._is_implicit_bool = True
            self._rule_type = RuleType.ValidateExpr

    def to_string(self):
        return self._jRule.toString()

    def boundaries(self):
        return self._boundaries

    def valid_numerics(self):
        return self._valid_numerics

    def valid_strings(self):
        return self._valid_strings

    def valid_expr(self):
        return self._valid_expr

    def is_implicit_bool(self):
        return self._jRule.implicitBoolean

    def ignore_case(self):
        return self._jRule.ignoreCase

    def invert_match(self):
        return self._jRule.invertMatch

    def rule_name(self):
        return self._jRule.ruleName

    def is_agg(self):
        return self._jRule.isAgg

    def input_column_name(self):
        return self._jRule.inputColumnName

    def rule_type(self):
        return self._rule_type

    def to_java(self):
        return self._jRule
