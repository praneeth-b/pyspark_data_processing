from pyspark.sql import SparkSession
import os
import logging
from pyspark.sql.functions import current_timestamp, lit

class RawDataLoader:
    """
    The Raw data from the json files defined in the config file are loaded as parquet files into the bronze layer.

    """
    def __init__(self, spark: SparkSession, raw_path: str, bronze_path: str):
        self.spark = spark
        self.raw_path = raw_path
        self.bronze_path = bronze_path
        self.logger = logging.getLogger(__name__)

    def load_json_to_bronze(self, dataset_name: str):
        """Load JSON and save as Parquet in bronze layer"""
        try:
            json_file = os.path.join(self.raw_path, f"yelp_academic_dataset_{dataset_name}.json")
            bronze_table = os.path.join(self.bronze_path, dataset_name)

            self.logger.info(f"Loading {dataset_name} from {json_file}")

            # Read Json file
            df = self.spark.read.json(json_file)

            # Add metadata columns to track the ingested timestamp and source file.
            df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_file", lit(json_file))

            # Write to bronze as Parquet format. No data modification/filter/clean performed at this step.
            df.write.mode("overwrite").parquet(bronze_table)

            row_count = df.count()
            self.logger.info(f"Loaded {row_count} rows to bronze/{dataset_name}")
            return row_count

        except Exception as e:
            self.logger.error(f"Failed to load {dataset_name}: {str(e)}")
            raise

    def load_all_datasets(self, datasets: list):
        """Load all the files specified in config file. The datasets(json files) of the Yelp dataset are defined in
           the config.yaml file

        """
        results = {}
        for dataset in datasets:
            results[dataset] = self.load_json_to_bronze(dataset)
        return results