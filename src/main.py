"""
Main ETL Pipeline Module
========================

Orchestrates the complete ETL workflow:
Extract -> Transform -> Load

Can be run standalone or triggered by Airflow.
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from .extract.api_connector import APIConnector
from .extract.csv_loader import CSVLoader
from .transform.cleaners import OrdersCleaner, ProductsCleaner, OrderItemsCleaner
from .transform.validators import (
    create_orders_validator, 
    create_order_items_validator,
    ValidationReport
)
from .load.db_loader import DatabaseLoader
from .utils.config import config
from .utils.logger import ETLLogger


class ETLPipeline:
    """
    Complete ETL Pipeline orchestrator.
    
    Coordinates extraction from multiple sources, transformation,
    validation, and loading into the data warehouse.
    
    Example:
        pipeline = ETLPipeline()
        results = pipeline.run(source="api")
    """
    
    def __init__(self, db_type: Optional[str] = None):
        """
        Initialize the ETL pipeline.
        
        Args:
            db_type: Database type ('sqlite' or 'postgresql')
        """
        self.logger = ETLLogger("pipeline")
        
        # Initialize components
        self.api_connector = APIConnector()
        self.csv_loader = CSVLoader()
        self.db_loader = DatabaseLoader(db_type=db_type)
        
        # Cleaners
        self.orders_cleaner = OrdersCleaner()
        self.products_cleaner = ProductsCleaner()
        self.order_items_cleaner = OrderItemsCleaner()
        
        # Validators
        self.orders_validator = create_orders_validator()
        self.order_items_validator = create_order_items_validator()
    
    def run(
        self,
        source: str = "api",
        validate: bool = True,
        save_raw: bool = True
    ) -> dict:
        """
        Execute the complete ETL pipeline.
        
        Args:
            source: Data source ('api', 'csv', or 'both')
            validate: Whether to run validation checks
            save_raw: Whether to save raw extracted data
            
        Returns:
            Dictionary with pipeline execution results
        """
        start_time = time.time()
        self.logger.info(f"Starting ETL pipeline", source=source)
        
        results = {
            "start_time": datetime.now().isoformat(),
            "source": source,
            "tables": {},
            "validation_reports": {},
            "errors": []
        }
        
        try:
            # Initialize database schema
            self.db_loader.initialize_schema()
            
            # EXTRACT
            self.logger.info("=== EXTRACT PHASE ===")
            raw_data = self._extract(source, save_raw)
            
            # TRANSFORM
            self.logger.info("=== TRANSFORM PHASE ===")
            cleaned_data = self._transform(raw_data)
            
            # VALIDATE
            if validate:
                self.logger.info("=== VALIDATE PHASE ===")
                validation_results = self._validate(cleaned_data)
                results["validation_reports"] = validation_results
                
                # Check for critical failures
                for table, report in validation_results.items():
                    if report.get("has_critical_failures"):
                        raise ValueError(
                            f"Critical validation failure in {table}"
                        )
            
            # LOAD
            self.logger.info("=== LOAD PHASE ===")
            load_results = self._load(cleaned_data)
            results["tables"] = load_results
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            results["errors"].append(str(e))
            results["status"] = "failed"
            raise
        
        # Calculate duration
        duration = time.time() - start_time
        results["duration_seconds"] = round(duration, 2)
        results["status"] = "completed"
        results["end_time"] = datetime.now().isoformat()
        
        self.logger.info(
            "Pipeline completed successfully",
            duration=f"{duration:.2f}s",
            tables=list(results["tables"].keys())
        )
        
        return results
    
    def _extract(self, source: str, save_raw: bool) -> dict[str, pd.DataFrame]:
        """Extract data from specified source(s)."""
        data = {}
        
        if source in ["api", "both"]:
            self.logger.info("Extracting from Fake Store API")
            api_data = self.api_connector.fetch_all(save_raw=save_raw)
            data["products_api"] = api_data.get("products", pd.DataFrame())
            data["orders_api"] = api_data.get("orders", pd.DataFrame())
            data["users_api"] = api_data.get("users", pd.DataFrame())
        
        if source in ["csv", "both"]:
            self.logger.info("Extracting from Olist CSV files")
            olist_path = Path(config.paths.get("olist_data", "data/raw/olist"))
            
            if olist_path.exists():
                csv_data = self.csv_loader.load_all_olist()
                data.update({f"{k}_csv": v for k, v in csv_data.items()})
            else:
                self.logger.warning(
                    "Olist data directory not found",
                    path=str(olist_path)
                )
        
        self.logger.info(f"Extraction complete", tables=list(data.keys()))
        return data
    
    def _transform(self, raw_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Transform and clean extracted data."""
        cleaned = {}
        
        # Process products
        for key in raw_data:
            if "products" in key:
                cleaned["products"] = self.products_cleaner.clean(raw_data[key])
                break
        
        # Process orders
        for key in raw_data:
            if "orders" in key and "items" not in key:
                cleaned["orders"] = self.orders_cleaner.clean(raw_data[key])
                break
        
        # Process order items
        for key in raw_data:
            if "order_items" in key or "items" in key:
                cleaned["order_items"] = self.order_items_cleaner.clean(raw_data[key])
                break
        
        # Process customers (from users or customers)
        for key in raw_data:
            if "users" in key or "customers" in key:
                df = raw_data[key].copy()
                # Rename user_id to customer_id if needed
                if "user_id" in df.columns and "customer_id" not in df.columns:
                    df = df.rename(columns={"user_id": "customer_id"})
                cleaned["customers"] = df
                break
        
        # Process sellers if available
        for key in raw_data:
            if "sellers" in key:
                cleaned["sellers"] = raw_data[key]
                break
        
        self.logger.info(
            "Transformation complete",
            tables=list(cleaned.keys())
        )
        
        return cleaned
    
    def _validate(self, cleaned_data: dict[str, pd.DataFrame]) -> dict[str, dict]:
        """Validate cleaned data quality."""
        reports = {}
        
        if "orders" in cleaned_data:
            report = self.orders_validator.validate(cleaned_data["orders"])
            reports["orders"] = report.to_dict()
        
        if "order_items" in cleaned_data:
            report = self.order_items_validator.validate(cleaned_data["order_items"])
            reports["order_items"] = report.to_dict()
        
        return reports
    
    def _load(self, cleaned_data: dict[str, pd.DataFrame]) -> dict[str, int]:
        """Load cleaned data into database."""
        load_counts = {}
        
        # Load in order of dependencies
        load_order = ["customers", "sellers", "products", "orders", "order_items"]
        
        for table in load_order:
            if table in cleaned_data and not cleaned_data[table].empty:
                loader_method = getattr(
                    self.db_loader, 
                    f"load_{table}", 
                    self.db_loader.load_dataframe
                )
                
                if hasattr(loader_method, "__call__"):
                    if table in ["customers", "sellers", "products", "orders", "order_items"]:
                        count = loader_method(cleaned_data[table])
                    else:
                        count = self.db_loader.load_dataframe(
                            cleaned_data[table], 
                            table
                        )
                    load_counts[table] = count
        
        return load_counts
    
    def run_api_pipeline(self) -> dict:
        """Convenience method to run API-only pipeline."""
        return self.run(source="api", validate=True)
    
    def run_csv_pipeline(self) -> dict:
        """Convenience method to run CSV-only pipeline."""
        return self.run(source="csv", validate=True)
    
    def run_full_pipeline(self) -> dict:
        """Convenience method to run pipeline with all sources."""
        return self.run(source="both", validate=True)


def main():
    """CLI entry point for the ETL pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="LogiFlow ETL Pipeline"
    )
    parser.add_argument(
        "--source",
        choices=["api", "csv", "both"],
        default="api",
        help="Data source to use"
    )
    parser.add_argument(
        "--db",
        choices=["sqlite", "postgresql"],
        default="sqlite",
        help="Database type"
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation step"
    )
    
    args = parser.parse_args()
    
    pipeline = ETLPipeline(db_type=args.db)
    results = pipeline.run(
        source=args.source,
        validate=not args.no_validate
    )
    
    print("\n" + "=" * 50)
    print("ETL Pipeline Results")
    print("=" * 50)
    print(f"Status: {results['status']}")
    print(f"Duration: {results['duration_seconds']}s")
    print(f"Tables loaded: {list(results['tables'].keys())}")
    
    for table, count in results["tables"].items():
        print(f"  - {table}: {count} rows")
    
    if results.get("errors"):
        print(f"\nErrors: {results['errors']}")


if __name__ == "__main__":
    main()
