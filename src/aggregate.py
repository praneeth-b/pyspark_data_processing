from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, avg, date_trunc,
    weekofyear, year, round as spark_round, when
)
import logging


class DataAggregator:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def aggregate_weekly_stars(self, reviews_df: DataFrame, business_df: DataFrame) -> DataFrame:
        """
        Aggregate stars per business on a weekly basis.
        Input: reviews_df: The siver layer reviews dataframe
               business_df: The siver layer business dataframe

        """
        self.logger.info("Aggregating weekly stars per business")

        # perform weekly aggregation on reviews dataframe
        weekly_agg = reviews_df.withColumn(
            "review_week", date_trunc("week", col("review_date"))
        ).withColumn(
            "review_year", year(col("review_date"))
        ).withColumn(
            "review_week_number", weekofyear(col("review_date"))
        ).groupBy(
            "business_id", "review_year", "review_week_number", "review_week"
            # review_week is redundant but might be useful for edge cases.
        ).agg(
            spark_round(avg("stars"), 2).alias("avg_stars_weekly"),
            count("review_id").alias("review_count_weekly"),
            spark_round(avg("useful"), 2).alias("avg_useful"),
            spark_round(avg("funny"), 2).alias("avg_funny"),
            spark_round(avg("cool"), 2).alias("avg_cool")
        )

        # Join with business info
        result = (weekly_agg.join(
            business_df.select("business_id", "business_name", "city", "state"),
            "business_id",
            "left"
        ).select(
            "business_id",
            "business_name",
            "city",
            "state",
            "review_week",
            "review_year",
            "review_week_number",
            "avg_stars_weekly",
            "review_count_weekly",
            "avg_useful",
            "avg_funny",
            "avg_cool"
        ).orderBy("business_id", "review_year", "review_week_number"))

        return result

    def aggregate_checkins_vs_stars(
            self, checkin_df: DataFrame, business_df: DataFrame) -> DataFrame:
        """
        Aggregate checkins per business compared to overall star rating
        Input: checkin_df: The siver layer checkins dataframe
               business_df: The siver layer business dataframe

        """
        self.logger.info("Aggregating check-ins vs star ratings")

        # Count check-ins per business
        checkin_counts = checkin_df.groupBy("business_id").agg(
            count("checkin_timestamp").alias("total_checkins")
        )

        # Join with business ratings
        result = business_df.join(
            checkin_counts,
            "business_id",
            "left"
        ).select(
            "business_id",
            "business_name",
            "city",
            "state",
            "stars",
            "review_count",
            when(col("total_checkins").isNull(), 0)
            .otherwise(col("total_checkins")).alias("total_checkins")
        ).withColumn(
            "checkins_per_review",
            spark_round(col("total_checkins") / col("review_count"), 2)
        ).withColumn(
            "star_category",  # adding an additional attribute based on star category
            when(col("stars") >= 4.0, "High")
            .when(col("stars") >= 2.5, "Medium")
            .otherwise("Low")
        )

        return result.orderBy(col("total_checkins").desc())
