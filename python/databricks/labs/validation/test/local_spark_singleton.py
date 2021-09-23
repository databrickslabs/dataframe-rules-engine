from pyspark.sql import SparkSession


class SparkSingleton:
    """A singleton class which returns one Spark instance"""
    __instance = None

    @classmethod
    def get_instance(cls):
        """Create a Spark instance.
        :return: A Spark instance
        """
        return (SparkSession.builder
                .getOrCreate())

    @classmethod
    def get_local_instance(cls):
        return (SparkSession.builder
                .master("local[*]")
                .appName("DataFrame Rules Engine")
                .getOrCreate())
