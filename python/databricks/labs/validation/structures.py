import pyspark
from typing import List


class Bounds:

    def __init__(self,
                 lower: float,
                 upper: float,
                 lower_inclusive: bool = False,
                 upper_inclusive: bool = False):
        self.lower = lower
        self.upper = upper
        self.lower_inclusive = lower_inclusive
        self.upper_inclusive = upper_inclusive

    def validation_logic(self, column: pyspark.sql.Column):
        lower_logic = (column >= self.lower) if self.lower_inclusive else (column > self.lower)
        upper_logic = (column <= self.upper) if self.upper_inclusive else (column < self.upper)
        return lower_logic and upper_logic


class MinMaxRuleDef:

    def __init__(self,
                 rule_name: str,
                 column: pyspark.sql.Column,
                 bounds: Bounds,
                 by: List[pyspark.sql.Column] = None):
        self.rule_name = rule_name
        self.column = column
        self.bounds = bounds
        self.by = by


class ValidationResults:

    def __init__(self,
                 complete_report: pyspark.sql.DataFrame,
                 summary_report: pyspark.sql.DataFrame):
        self.complete_report = complete_report
        self.summary_report = summary_report
