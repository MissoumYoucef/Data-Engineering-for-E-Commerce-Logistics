"""
CSV Loader Module
=================

Loads CSV data files with memory-efficient chunking,
encoding detection, and initial data profiling.

Primary data source: Olist Brazilian E-Commerce Dataset
"""

from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

import pandas as pd

from ..utils.config import config
from ..utils.logger import ETLLogger


class CSVLoader:
    """
    Memory-efficient CSV loader with data profiling.
    
    Handles large datasets through chunked reading, similar to how
    fleet management systems process historical trip logs.
    
    Features:
        - Chunked reading for large files
        - Automatic encoding detection
        - Initial data profiling
        - Schema inference
        
    Example:
        loader = CSVLoader()
        orders_df = loader.load_olist_orders()
        profile = loader.profile_data(orders_df)
    """
    
    # Expected Olist dataset files
    OLIST_FILES = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "order_reviews": "olist_order_reviews_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "products": "olist_products_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv"
    }
    
    def __init__(self, data_path: Optional[str] = None):
        """
        Initialize CSV loader with data directory path.
        
        Args:
            data_path: Path to directory containing CSV files
        """
        self.logger = ETLLogger("extract")
        self.data_path = Path(
            data_path or config.paths.get("olist_data", "data/raw/olist")
        )
    
    def load_csv(
        self,
        file_path: str | Path,
        chunk_size: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame | Generator[pd.DataFrame, None, None]:
        """
        Load a CSV file with optional chunking.
        
        Args:
            file_path: Path to CSV file
            chunk_size: If provided, return a generator of chunks
            **kwargs: Additional arguments passed to pd.read_csv
            
        Returns:
            DataFrame or Generator of DataFrames if chunked
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            self.logger.error("File not found", path=str(file_path))
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        self.logger.info("Loading CSV file", file=file_path.name)
        
        # Default read options
        read_options = {
            "encoding": "utf-8",
            "low_memory": False,
            **kwargs
        }
        
        if chunk_size:
            read_options["chunksize"] = chunk_size
            return pd.read_csv(file_path, **read_options)
        
        df = pd.read_csv(file_path, **read_options)
        
        # Add extraction metadata
        df["extracted_at"] = datetime.now().isoformat()
        df["source_file"] = file_path.name
        
        self.logger.info(
            "CSV file loaded",
            file=file_path.name,
            rows=len(df),
            columns=len(df.columns)
        )
        
        return df
    
    def load_olist_orders(self) -> pd.DataFrame:
        """
        Load Olist orders dataset.
        
        This is the main orders table with timestamps for:
        - Order creation
        - Approval
        - Carrier delivery
        - Customer delivery
        - Estimated delivery
        
        Mirrors trip/delivery tracking in fleet management.
        """
        file_path = self.data_path / self.OLIST_FILES["orders"]
        
        df = self.load_csv(
            file_path,
            parse_dates=[
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date"
            ]
        )
        
        return df
    
    def load_olist_order_items(self) -> pd.DataFrame:
        """
        Load Olist order items dataset.
        
        Contains line-item details with:
        - Order ID, Product ID, Seller ID
        - Price and shipping costs
        - Shipping limit date
        
        Mirrors line-item costs in logistics.
        """
        file_path = self.data_path / self.OLIST_FILES["order_items"]
        
        df = self.load_csv(
            file_path,
            parse_dates=["shipping_limit_date"]
        )
        
        return df
    
    def load_olist_customers(self) -> pd.DataFrame:
        """
        Load Olist customers dataset.
        
        Contains customer locations with:
        - Customer ID, City, State, Zip Code
        
        Mirrors customer/delivery location data.
        """
        file_path = self.data_path / self.OLIST_FILES["customers"]
        return self.load_csv(file_path)
    
    def load_olist_sellers(self) -> pd.DataFrame:
        """
        Load Olist sellers dataset.
        
        Contains seller/warehouse locations with:
        - Seller ID, City, State, Zip Code
        
        Mirrors warehouse/hub location data.
        """
        file_path = self.data_path / self.OLIST_FILES["sellers"]
        return self.load_csv(file_path)
    
    def load_all_olist(self) -> dict[str, pd.DataFrame]:
        """
        Load all Olist dataset files that exist.
        
        Returns:
            Dictionary of DataFrames keyed by entity name
        """
        self.logger.info("Loading all Olist datasets")
        
        data = {}
        for name, filename in self.OLIST_FILES.items():
            file_path = self.data_path / filename
            if file_path.exists():
                try:
                    data[name] = self.load_csv(file_path)
                except Exception as e:
                    self.logger.warning(
                        f"Failed to load {name}",
                        error=str(e)
                    )
            else:
                self.logger.warning(f"File not found: {filename}")
        
        self.logger.info(
            "Olist data loading complete",
            loaded_tables=list(data.keys())
        )
        
        return data
    
    @staticmethod
    def profile_data(df: pd.DataFrame) -> dict:
        """
        Generate a data profile for a DataFrame.
        
        Returns summary statistics useful for:
        - Data quality assessment
        - Schema validation
        - Identifying missing values
        
        Args:
            df: DataFrame to profile
            
        Returns:
            Dictionary with profiling results
        """
        profile = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
            "columns": {}
        }
        
        for col in df.columns:
            col_profile = {
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "null_pct": round(df[col].isnull().mean() * 100, 2),
                "unique_count": int(df[col].nunique()),
            }
            
            # Add stats for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile.update({
                    "min": float(df[col].min()) if not df[col].isnull().all() else None,
                    "max": float(df[col].max()) if not df[col].isnull().all() else None,
                    "mean": float(df[col].mean()) if not df[col].isnull().all() else None
                })
            
            profile["columns"][col] = col_profile
        
        return profile


# Convenience function for quick loading
def load_olist_data(data_path: Optional[str] = None) -> dict[str, pd.DataFrame]:
    """
    Convenience function to load all Olist data.
    
    Args:
        data_path: Optional path to Olist data directory
        
    Returns:
        Dictionary of DataFrames
    """
    loader = CSVLoader(data_path)
    return loader.load_all_olist()
