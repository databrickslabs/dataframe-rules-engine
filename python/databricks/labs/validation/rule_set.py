from pyspark.sql import DataFrame

from databricks.labs.validation.local_spark_singleton import SparkSingleton


class RuleSet():

    def __init__(self, df):
        self._spark = SparkSingleton.get_instance()
        self._df = df
        self._jdf = df._jdf
        self._jRuleSet = self._spark._jvm.com.databricks.labs.validation.RuleSet.apply(self._jdf)

    def add(self, rule):
        self._jRuleSet.add(rule.to_java())

    def get_df(self):
        return self._df

    def to_java(self):
        return self._jRuleSet

    def get_complete_report(self):
        jCompleteReport = self._jRuleSet.getCompleteReport
        return DataFrame(jCompleteReport, self._spark)

    def get_summary_report(self):
        jSummaryReport = self._jRuleSet.getSummaryReport
        return DataFrame(jSummaryReport, self._spark)
