"""
Airflow DAG for LogiFlow ETL Pipeline
======================================

Schedules and orchestrates the ETL pipeline with:
- Daily execution at 2 AM
- Retry logic with exponential backoff
- Task dependencies and parallelism
- Alerting on failure
"""

from datetime import datetime, timedelta
import pendulum
from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator

# Default arguments for all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def extract_api_data(**context):
    """
    Extract data from Fake Store API.
    
    This task fetches products, orders, and users from the API
    and saves raw data to the data/raw directory.
    """
    from src.extract.api_connector import APIConnector
    
    connector = APIConnector()
    data = connector.fetch_all(save_raw=True)
    
    # Push extracted counts to XCom for downstream tasks
    context["ti"].xcom_push(key="products_count", value=len(data["products"]))
    context["ti"].xcom_push(key="orders_count", value=len(data["orders"]))
    context["ti"].xcom_push(key="users_count", value=len(data["users"]))
    
    return {
        "products": len(data["products"]),
        "orders": len(data["orders"]),
        "users": len(data["users"])
    }


def extract_csv_data(**context):
    """
    Extract data from Olist CSV files.
    
    This task loads all available Olist dataset files
    into DataFrames for transformation.
    """
    from src.extract.csv_loader import CSVLoader
    from pathlib import Path
    
    loader = CSVLoader()
    olist_path = Path("data/raw/olist")
    
    if not olist_path.exists():
        print("Olist data not found, skipping CSV extraction")
        return {"tables_loaded": 0}
    
    data = loader.load_all_olist()
    
    context["ti"].xcom_push(key="csv_tables", value=list(data.keys()))
    
    return {"tables_loaded": len(data)}


def transform_data(**context):
    """
    Transform and clean extracted data.
    
    Applies cleaning logic:
    - Remove duplicates
    - Handle missing values
    - Standardize timestamps
    - Calculate derived metrics
    """
    import pandas as pd
    from pathlib import Path
    from src.transform.cleaners import (
        OrdersCleaner, ProductsCleaner, OrderItemsCleaner
    )
    
    raw_path = Path("data/raw")
    processed_path = Path("data/processed")
    processed_path.mkdir(parents=True, exist_ok=True)
    
    # Load and clean products
    products_file = raw_path / "products_raw.csv"
    if products_file.exists():
        products_df = pd.read_csv(products_file)
        cleaner = ProductsCleaner()
        cleaned = cleaner.clean(products_df)
        cleaned.to_csv(processed_path / "products_cleaned.csv", index=False)
    
    # Load and clean orders
    orders_file = raw_path / "orders_raw.csv"
    if orders_file.exists():
        orders_df = pd.read_csv(orders_file)
        cleaner = OrdersCleaner()
        cleaned = cleaner.clean(orders_df)
        cleaned.to_csv(processed_path / "orders_cleaned.csv", index=False)
    
    return {"status": "transform_complete"}


def validate_data(**context):
    """
    Validate data quality.
    
    Runs validation checks:
    - Null percentage thresholds
    - Uniqueness constraints
    - Business rule validation
    """
    import pandas as pd
    from pathlib import Path
    from src.transform.validators import (
        create_orders_validator, create_order_items_validator
    )
    
    processed_path = Path("data/processed")
    validation_results = {}
    
    # Validate orders
    orders_file = processed_path / "orders_cleaned.csv"
    if orders_file.exists():
        orders_df = pd.read_csv(orders_file)
        validator = create_orders_validator()
        report = validator.validate(orders_df)
        validation_results["orders"] = report.to_dict()
        
        if report.has_critical_failures:
            raise ValueError("Critical validation failures in orders data")
    
    context["ti"].xcom_push(key="validation_results", value=validation_results)
    
    return validation_results


def load_data(**context):
    """
    Load cleaned data into the database.
    
    Performs upsert operations for idempotent loads.
    """
    import pandas as pd
    from pathlib import Path
    from src.load.db_loader import DatabaseLoader
    
    loader = DatabaseLoader(db_type="sqlite")
    loader.initialize_schema()
    
    processed_path = Path("data/processed")
    load_counts = {}
    
    # Load products
    products_file = processed_path / "products_cleaned.csv"
    if products_file.exists():
        df = pd.read_csv(products_file)
        count = loader.load_products(df)
        load_counts["products"] = count
    
    # Load orders
    orders_file = processed_path / "orders_cleaned.csv"
    if orders_file.exists():
        df = pd.read_csv(orders_file)
        count = loader.load_orders(df)
        load_counts["orders"] = count
    
    # Log ETL run
    total_loaded = sum(load_counts.values())
    loader.log_etl_run(
        table_name="all",
        source="api",
        rows_extracted=total_loaded,
        rows_transformed=total_loaded,
        rows_loaded=total_loaded,
        validation_passed=True,
        status="completed"
    )
    
    return load_counts


# Define the DAG
with DAG(
    dag_id="logiflow_etl_pipeline",
    default_args=default_args,
    description="LogiFlow E-Commerce Logistics ETL Pipeline",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "logistics", "data-engineering"],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id="start")
    
    # Extract tasks (can run in parallel)
    extract_api = PythonOperator(
        task_id="extract_api_data",
        python_callable=extract_api_data,
    )
    
    extract_csv = PythonOperator(
        task_id="extract_csv_data",
        python_callable=extract_csv_data,
    )
    
    # Transform task (depends on both extracts)
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )
    
    # Validate task
    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )
    
    # Load task
    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )
    
    # End marker
    end = EmptyOperator(task_id="end")
    
    # Define task dependencies
    # 
    # start -> [extract_api, extract_csv] -> transform -> validate -> load -> end
    #
    start >> [extract_api, extract_csv] >> transform >> validate >> load >> end
