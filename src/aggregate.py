from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, avg, date_trunc, coalesce, lit,
    weekofyear, year, round as spark_round, when
)
from pyspark.sql.types import TimestampType
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

        # define defaults to use in coalesce()
        default_week_number = 0
        default_year = 9999
        default_review_week = lit('9999-01-01 00:00:00').cast(TimestampType())

        result = (business_df.join(weekly_agg, "business_id", how= "left"
                                   ).select(
            "business_id",
            "business_name",
            "city",
            "state",
            col("stars").alias("overall_stars"),
            coalesce('review_week', default_review_week).alias('review_week'),
            coalesce('review_year', lit(default_year)).alias('review_year'),
            coalesce('review_week_number', lit(default_week_number)).alias('review_week_number'),
            coalesce('avg_stars_weekly', lit(0)).alias('avg_stars_weekly'),
            coalesce('review_count_weekly', lit(0)).alias('review_count_weekly'),
            coalesce('avg_useful', lit(0)).alias('avg_useful'),
            coalesce('avg_funny', lit(0)).alias('avg_funny'),
            coalesce('avg_cool', lit(0)).alias('avg_cool')
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
