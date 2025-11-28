from pyspark.sql import SparkSession
import logging


class SparkSessionManager:
    _instance = None

    @classmethod
    def get_spark_session(cls, app_name="YelpDataPipeline", driver_memory="4g", executor_memory="4g",shuffle_partitions="64"):
        if cls._instance is None:
            cls._instance = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")  # running on my local pc
                .config("spark.driver.memory", driver_memory)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.shuffle.partitions", shuffle_partitions)
                .config("spark.executor.memory", executor_memory)
                .config("spark.sql.autoBroadcastJoinThreshold", 200 * 1024 * 1024)
                .getOrCreate()
            )
            cls._instance.sparkContext.setLogLevel("WARN")
            logging.info(f"Spark session created: {app_name}")
        return cls._instance

    @classmethod
    def stop(cls):
        if cls._instance:
            cls._instance.stop()
            cls._instance = None