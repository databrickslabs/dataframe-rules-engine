from pyspark.sql import SparkSession
from pyspark import SparkConf
import os


class SparkSingleton:
    """A singleton class which returns one Spark instance"""
    __instance = None

    @classmethod
    def get_instance(cls):
        """Create a Spark instance.
        :return: A Spark instance
        """
        config = SparkConf().setAll([("spark.driver.extraClassPath",
                                      os.environ["RULES_ENGINE_JAR"])])
        return (SparkSession.builder
                .config(conf=config)
                .appName("DataFrame Rules Engine")
                .getOrCreate())

    @classmethod
    def get_local_instance(cls):
        config = SparkConf().setAll([("spark.driver.extraClassPath",
                                      os.environ["RULES_ENGINE_JAR"])])
        return (SparkSession.builder
                .config(conf=config)
                .master("local[*]")
                .appName("DataFrame Rules Engine")
                .getOrCreate())
