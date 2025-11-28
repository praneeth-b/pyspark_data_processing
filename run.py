from utils.spark_session import SparkSessionManager
from src.ingest import RawDataLoader
from src.clean import DataCleaner
from src.aggregate import DataAggregator
from utils.utilities import load_config, setup_logging, create_directories


def main():
    # Load configuration
    config = load_config()
    create_directories(config)
    logger = setup_logging(config['paths']['logs'])

    logger.info("#" * 80)
    logger.info("STARTING YELP DATA PIPELINE")
    logger.info("#" * 80)

    try:
        # create a spark session
        spark = SparkSessionManager.get_spark_session(
            app_name=config['spark']['app_name'],
            memory=config['spark']['memory']
        )

        # STAGE 1: Bronze - Load raw data
        logger.info("\n[STAGE 1] Loading raw JSON data to Bronze layer...")
        loader = RawDataLoader(
            spark,
            config['paths']['raw'],
            config['paths']['bronze']
        )
        load_results = loader.load_all_datasets(config['datasets'])
        logger.info(f"Bronze load complete: {load_results}")

        # Stage 2: Silver: Clean the raw data based on final aggregation.
        logger.info("\n[STAGE 2] Cleaning data to Silver layer...")
        cleaner = DataCleaner(spark)

        # Clean each dataset

        # clean business dataset
        business_bronze = spark.read.parquet(f"{config['paths']['bronze']}/business")
        business_silver = cleaner.clean_business(business_bronze)
        business_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/business")
        business_silver.cache()  # caching business_silver since it is used multiple times in gold layer.
        logger.info(f"Cleaned business: {business_silver.count()} rows")  # also materialize cache()


        # clean review_dataset
        review_bronze = spark.read.parquet(f"{config['paths']['bronze']}/review")
        review_silver = cleaner.clean_review(review_bronze)
        review_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/review")
        logger.info(f"Cleaned review: {review_silver.count()} rows")
        # review_silver.show(10)

        # clean checkin dataset
        checkin_bronze = spark.read.parquet(f"{config['paths']['bronze']}/checkin")
        checkin_silver = cleaner.clean_checkin(checkin_bronze)
        checkin_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/checkin")
        logger.info(f"Cleaned checkin: {checkin_silver.count()} rows")
        checkin_silver.cache()
        # checkin_silver.show(10)

        # clean user dataset
        user_bronze = spark.read.parquet(f"{config['paths']['bronze']}/user")
        user_silver = cleaner.clean_user(user_bronze)
        user_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/user")
        logger.info(f"Cleaned user: {user_silver.count()} rows")
        # user_silver.show(10)

        tip_bronze = spark.read.parquet(f"{config['paths']['bronze']}/tip")
        tip_silver = cleaner.clean_tip(tip_bronze)
        tip_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/tip")
        logger.info(f"Cleaned tip: {tip_silver.count()} rows")

        # STAGE 3: GOLD - Aggregate data
        logger.info("\n[STAGE 3] Creating aggregations in Gold layer...")
        aggregator = DataAggregator(spark)

        # Weekly stars aggregation
        weekly_stars = aggregator.aggregate_weekly_stars(review_silver, business_silver)
        # partition by 'review_week' column before write for efficient querying later.
        weekly_stars.write.partitionBy("review_week").mode("overwrite").parquet(f"{config['paths']['gold']}/weekly_stars")
        logger.info(f"Weekly stars aggregation: {weekly_stars.count()} rows")

        # Check-ins vs stars aggregation
        checkins_vs_stars = aggregator.aggregate_checkins_vs_stars(checkin_silver, business_silver)
        checkins_vs_stars.write.mode("overwrite").parquet(f"{config['paths']['gold']}/checkins_vs_stars")
        logger.info(f"Check-ins vs stars aggregation: {checkins_vs_stars.count()} rows")


    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise
    finally:
        SparkSessionManager.stop()


if __name__ == "__main__":
    main()