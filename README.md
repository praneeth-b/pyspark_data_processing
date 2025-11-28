# pyspark_data_processing
A production-grade data engineering solution for processing Yelp's academic dataset using PySpark and the Medallion Architecture (Bronze->Silver->Gold layers).

## Architecture Design

### Design Decisions

#### 1. Medallion Architecture (Bronze-Silver-Gold)
- **Bronze Layer**: Raw data ingestion from JSON files, stored as Parquet for better performance
- **Silver Layer**: Cleaned, validated, and deduplicated data with proper schema enforcement, stored as Parquet format.
- **Gold Layer**: Business-ready aggregations optimized for analytics, stored as Parquet format.

**Rationale**: This architecture provides data quality gates, enables incremental processing, and separates concerns between raw ingestion, cleaning, and business logic.

#### 2. Parquet Storage Format
All layers (Bronze, Silver and Gold) use Parquet instead of JSON for:
- Columnar storage enabling faster analytical queries.
- Better compression in columnar storage.
- Better integration with Spark.

#### 3. PySpark Over Pandas
- Handles large datasets that don't fit in memory
- Lazy evaluation enables query optimization
- Scales horizontally if moved to cluster
- Native support for distributed computing

#### 4. Modular Python Project Structure
Organized as a proper Python package rather than notebooks:
- Better code reusability and testability
- Easier version control and collaboration
- Separation of concerns (ingestion, cleaning, aggregation)
- Easier for deployment

#### 5. Configuration-Driven Design
All paths, datasets, and parameters defined in `config.yaml`:
- Avoid hardcoded values in code
- Centralized configuration management


## Project Structure

```
pyspark_data_processing/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── run.py                             # File to run the entire data pipeline
├── Description.ipynb                  # Notebook to demonstrate the cleaning and aggregation logic
├── config/
│   └── config.yaml                    # Configuration file
├── src/
│   └── yelp_pipeline/
│       ├── __init__.py
│       ├── aggregate.py                # Aggregation code: Gold layer                
│       ├── clean.py                    # Cleaning code: Silver layer
│       └──  ingest.py                  # Ingestion code: Bronze layer
├── utils/
│    ├──utilities.py                    # Generic utility functions
│    └── spark_session.py               # Spark session manager implementation
│
│               
├── tests/
│   └── Analysis.ipynb                # Random testing
├── data/
│   ├── raw/                           # Original JSON files
│   ├── bronze/                        # Raw Parquet 
│   ├── silver/                        # Cleaned data Parquet
│   └── gold/                          # Aggregated data Parquet
└── logs/                              # Pipeline execution logs
```

## Prerequisites

- Python 3.11+ (tested on Python 3.13.9)
- Java 11 (openjdk@11) (required for PySpark)
- Spark 3.5.7 : https://spark.apache.org/docs//3.5.7/
- 8GB+ RAM recommended




## Installation & Setup

### Step 1: Clone or unbundle the repository

```bash
cd pyspark_data_processing
```

### Step 2: Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 5: Download Yelp Dataset

1. Visit: https://www.yelp.com/dataset/download
2. Extract all JSON files to `data/raw/` directory

Expected files:
```
pyspark_data_processing/data/raw/
├── yelp_academic_dataset_business.json
├── yelp_academic_dataset_review.json
├── yelp_academic_dataset_user.json
├── yelp_academic_dataset_checkin.json
└── yelp_academic_dataset_tip.json
```

## Running the Pipeline


```bash
# Make sure you're in the project root and venv is activated
python run.py
```
Expected output:
```
(venv_spark) praneeth@Mac pyspark_data_processing % python run.py
2025-11-28 09:44:42,750 - utils.utilities - INFO - ################################################################################
2025-11-28 09:44:42,750 - utils.utilities - INFO - STARTING YELP DATA PIPELINE
################################################################################
[STAGE 1] Loading raw JSON data to Bronze layer...
2025-11-28 09:44:44,040 - src.ingest - INFO - Loading business from data/raw/yelp_academic_dataset_business.json
2025-11-28 09:44:48,878 - src.ingest - INFO - Loaded 150346 rows to bronze/business
...
[STAGE 2] Cleaning data to Silver layer...
2025-11-28 09:45:13,010 - src.clean - INFO - Cleaning business data
2025-11-28 09:45:14,202 - utils.utilities - INFO - Cleaned business: 150346 rows
2025-11-28 09:45:14,258 - src.clean - INFO - Cleaning review data
...
[STAGE 3] Creating aggregations in Gold layer...
2025-11-28 09:45:42,117 - src.aggregate - INFO - Aggregating weekly stars per business
2025-11-28 09:45:55,835 - utils.utilities - INFO - Weekly stars aggregation: 5138585 rows
...
```
