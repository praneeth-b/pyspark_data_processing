

import os
import yaml
import logging
from datetime import datetime
from utils.spark_session import SparkSessionManager
from src.ingest import RawDataLoader
from src.clean import DataCleaner


def setup_logging(log_dir: str):
    """ Setup logging configuration
        Adding handlers for file handling for writing to .log file and Streaming logs on the console.

    """
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def load_config(config_path: str = "config/config.yaml"):
    """Read configuration from YAML"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_directories(config: dict):
    """Create necessary directories"""
    for path_key in ['bronze', 'silver', 'gold', 'logs']:
        os.makedirs(config['paths'][path_key], exist_ok=True)


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
        # load_results = loader.load_all_datasets(config['datasets'])
        # logger.info(f"Bronze load complete: {load_results}")
        # Skipping load for testing.

        # Stage 2: Silver: Clean the raw data based on final aggregation.
        logger.info("\n[STAGE 2] Cleaning data to Silver layer...")
        cleaner = DataCleaner(spark)

        # Clean each dataset
        business_bronze = spark.read.parquet(f"{config['paths']['bronze']}/business")
        business_silver = cleaner.clean_business(business_bronze)
        business_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/business")
        logger.info(f"Cleaned business: {business_silver.count()} rows")
        # business_silver.show(10)

        review_bronze = spark.read.parquet(f"{config['paths']['bronze']}/review")
        review_silver = cleaner.clean_review(review_bronze)
        review_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/review")
        logger.info(f"Cleaned review: {review_silver.count()} rows")
        # review_silver.show(10)

        checkin_bronze = spark.read.parquet(f"{config['paths']['bronze']}/checkin")
        checkin_silver = cleaner.clean_checkin(checkin_bronze)
        checkin_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/checkin")
        logger.info(f"Cleaned checkin: {checkin_silver.count()} rows")
        # checkin_silver.show(10)


        user_bronze = spark.read.parquet(f"{config['paths']['bronze']}/user")
        user_silver = cleaner.clean_user(user_bronze)
        user_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/user")
        logger.info(f"Cleaned user: {user_silver.count()} rows")
        # user_silver.show(10)

        # from pyspark.sql.functions import col
        tip_bronze = spark.read.parquet(f"{config['paths']['bronze']}/tip")
        tip_silver = cleaner.clean_tip(tip_bronze)
        tip_silver.write.mode("overwrite").parquet(f"{config['paths']['silver']}/tip")
        logger.info(f"Cleaned tip: {tip_silver.count()} rows")




    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise
    finally:
        SparkSessionManager.stop()


if __name__ == "__main__":
    main()