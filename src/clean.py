from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, trim, regexp_replace, to_timestamp, to_date, explode, split
import logging


class DataCleaner:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def clean_business(self, df: DataFrame) -> DataFrame:
        """Clean the business dataset (source: yelp_academic_dataset_business.json)"""
        self.logger.info("Cleaning business data")

        """ The column selection follows the Silver Layer philosophy:        
            - Keep essential columns for downstream analytics and standardize data types
            - Remove overly nested/complex structures that need separate processing : Here Attributes column is highly 
              nested and does not have a consistent structure. Hence removing it out.
            - Check for Null values and valid range of stars column and deduplicate.
        
            """

        # selecting required columns and casting them to a suitable data type.
        column_selected_df = df.select(
            trim(col("business_id")).alias("business_id").cast("string"),
            trim(col("name")).alias("business_name").cast("string"),
            col("address").cast("string"),
            col("city").cast("string"),
            col("state").cast("string"),
            col("postal_code").cast("string"),
            col("latitude").cast("double"),
            col("longitude").cast("double"),
            col("stars").cast("double"),
            col("review_count").cast("int"),
            col("is_open").cast("int"),
            col("categories")
        )

        # Remove records with missing critical fields
        filtered_df = column_selected_df.filter(
            col("business_id").isNotNull() &
            col("business_name").isNotNull() &
            col("stars").isNotNull() &
            (col("stars").between(0, 5))  # Valid star range
        )

        # drop duplicate business_ids
        depuplicated_df = filtered_df.dropDuplicates(["business_id"])


        return depuplicated_df

    def clean_review(self, df: DataFrame) -> DataFrame:
        """Clean review dataset source: yelp_academic_dataset_review.json
             - select required columns and trim required columns for leading/lagging spaces and standardize data types.
            - Check for nulls and valid range for respective columns
            - Remove duplicate reviews
        """

        self.logger.info("Cleaning review data")

        column_selection_df =  df.select(
            trim(col("review_id")).alias("review_id").cast("string"),
            trim(col("user_id")).alias("user_id").cast("string"),
            trim(col("business_id")).alias("business_id").cast("string"),
            col("stars").cast("double"),
            col("useful").cast("int"),
            col("funny").cast("int"),
            col("cool").cast("int"),
            to_date(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")).alias("review_date"), # Convert to timestamp when it is string
            col("text").cast("string").alias("review_text")
        )

        # Remove records with missing critical fields
        filtered_df = column_selection_df.filter(
            col("review_id").isNotNull() &
            col("user_id").isNotNull() &
            col("business_id").isNotNull() &
            col("stars").isNotNull() &
            (col("stars").between(0, 5))
        )

        # drop duplicate review_id
        deduplicated_df = filtered_df.dropDuplicates(["review_id"])

        return deduplicated_df

    def clean_checkin(self, df: DataFrame) -> DataFrame:
        """ Clean checkin dataset  source: yelp_academic_dataset_checkin.json
             - Explode comma seperated checkin_time column to multiple rows and standardize data types.
            - Enrich with checkin_timestamp and checkin_date columns.
            - Check for Null values and deduplicate checkin data.

        """
        self.logger.info("Cleaning checkin data")


        # Explode comma-separated dates
        checkin_explode_df = df.select(
            col("business_id"),
            explode(split(col("date"), ", ")).alias("checkin_time")
        )

        # Enrich with checkin timestamp and checkin date columns
        checkin_column_format_df = (
            checkin_explode_df
        .withColumn(
            "checkin_timestamp", to_timestamp(col("checkin_time"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn(
            "checkin_date", to_date(col("checkin_timestamp")))
        )

        checkin_selected_df = checkin_column_format_df.select(
            trim(col("business_id")).alias("business_id").cast("string"),
            col("checkin_timestamp"),
            col("checkin_date")
        )

        # Remove records with missing critical fields
        checkin_filtered_df = checkin_selected_df.filter(
            col("business_id").isNotNull() &
            col("checkin_timestamp").isNotNull()
        )

        # remove duplicates
        checkin_deduplicated_df = checkin_filtered_df.dropDuplicates(["business_id", "checkin_timestamp"])

        return checkin_deduplicated_df

    def clean_user(self, df: DataFrame) -> DataFrame:
        """Clean user dataset  source: yelp_academic_dataset_user.json
            - Trim values and standardize data types
            - Select required columns
            - Check for nulls and deduplicate

        """
        self.logger.info("Cleaning user data")

        user_selected_df = df.select(
            trim(col("user_id")).alias("user_id").cast("string"),
            trim(col("name")).cast("string").alias("user_name"),
            col("review_count").cast("int"),
            to_date(to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss")).alias("yelping_since"),
            col("useful").cast("int"),
            col("funny").cast("int"),
            col("cool").cast("int"),
            col("fans").cast("int"),
            col("average_stars").cast("double")
        )

        # Remove records with missing critical fields
        user_filtered_df = user_selected_df.filter(
            col("user_id").isNotNull()
        )

        # remove duplicate user_id
        user_deduplicated_df = user_filtered_df.dropDuplicates(["user_id"])

        return user_deduplicated_df

    def clean_tip(self, df: DataFrame) -> DataFrame:
        """Clean tip dataset  source: yelp_academic_dataset_tip.json
            - Trim and standardize data types
            - Select required columns
            - Check for null values

        """
        self.logger.info("Cleaning tip data")

        tip_selected_df = df.select(
            trim(col("user_id")).alias("user_id").cast("string"),
            trim(col("business_id")).alias("business_id").cast("string"),
            col("text").cast("string").alias("tip_text"),
            to_date(to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss")).alias("tip_date"),
            col("compliment_count").cast("int")
        )

        tip_filtered_df = tip_selected_df.filter(
            col("user_id").isNotNull() &
            col("business_id").isNotNull()&
            col("tip_text").isNotNull()
        )

        return tip_filtered_df