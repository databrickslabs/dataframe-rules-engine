import pyspark

from databricks.labs.validation.local_spark_singleton import SparkSingleton
from databricks.labs.validation.structures import Bounds
from databricks.labs.validation.utils.helpers import Helpers

from typing import List


class Rule:
    """
    Definition of a rule
    """
    def __init__(self,
                 rule_name: str,
                 column: pyspark.sql.Column,
                 boundaries: Bounds = None,
                 valid_expr: pyspark.sql.Column = None,
                 valid_strings: List[str] = None,
                 valid_numerics: List[float] = None,
                 ignore_case: bool = False,
                 invert_match: bool = False):

        self.spark = SparkSingleton.get_instance()

        self.rule_name = rule_name
        self.column = column
        self.boundaries = boundaries
        self.valid_expr = valid_expr
        self.valid_strings = valid_strings
        self.valid_numerics = valid_numerics
        self.ignore_case = ignore_case
        self.invert_match = invert_match

        # Determine the Rule type by parsing the input arguments
        if len(valid_strings) > 0:
            j_valid_strings = Helpers.to_java_array(valid_strings, self.spark._sc)
            self._rule = self.spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name, column._jc,
                                                                                   j_valid_strings,
                                                                                   ignore_case, invert_match)
        elif len(valid_numerics) > 0:
            j_valid_strings = Helpers.to_java_array(valid_strings, self.spark._sc)
            self._rule = self.spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name, column._jc,
                                                                                   j_valid_strings,
                                                                                   ignore_case, invert_match)
        else:
            self._rule = self.spark._jvm.com.databricks.labs.validation.Rule.apply(rule_name, column._jc)

    def to_string(self):
        return self._rule.toString()

    def boundaries(self):
        return self._rule.boundaries

    def valid_numerics(self):
        return self.valid_numerics

    def valid_strings(self):
        return self.valid_strings

    def valid_expr(self):
        return self.valid_expr

    def is_implicit_bool(self):
        return self._rule.implicitBoolean

    def ignore_case(self):
        return self.ignore_case

    def invert_match(self):
        return self.invert_match

    def rule_name(self):
        return self._rule.inputRuleName

    def is_agg(self):
        return self._rule.isAgg()
