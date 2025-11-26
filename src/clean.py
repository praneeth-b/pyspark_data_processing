from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, trim, regexp_replace, to_timestamp
import logging


class DataCleaner:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def clean_business(self, df: DataFrame) -> DataFrame:
        """Clean the business dataset (source: yelp_academic_dataset_business.json)"""
        self.logger.info("Cleaning business data")

        """ The column selection follows the Silver Layer philosophy:        
            - Keep essential columns for downstream analytics
            - Remove overly nested/complex structures that need separate processing : Here Attributes column is highly 
              nested and does not have a consistent structure. Hence removing it out.
            - Standardize data types such that it is consitent.
        
            """


        column_selected_df = df.select(
            col("business_id"),
            trim(col("name")).alias("business_name"),
            col("address"),
            col("city"),
            col("state"),
            col("postal_code"),
            col("latitude"),
            col("longitude"),
            col("stars").cast("double"),
            col("review_count").cast("int"),
            col("is_open").cast("int"),
            col("categories")
        )


        filtered_df = column_selected_df.filter(
            # Remove records with missing critical fields
            col("business_id").isNotNull() &
            col("business_name").isNotNull() &
            col("stars").isNotNull() &
            (col("stars").between(0, 5))  # Valid star range
        )

        depuplicated_df = filtered_df.dropDuplicates(["business_id"])


        return depuplicated_df

