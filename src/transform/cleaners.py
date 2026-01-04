"""
Data Cleaners Module
====================

Pandas-based data transformations for cleaning and standardizing
raw data from various sources.

Handles:
- Missing values (timestamps, IDs)
- Duplicate records
- Data type conversions
- Derived metric calculations
"""

from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd
import numpy as np

from ..utils.logger import ETLLogger


class DataCleaner:
    """
    Data cleaning pipeline with configurable transformation steps.
    
    Applies a series of cleaning operations to raw DataFrames,
    similar to how fleet data is cleaned before analytics.
    
    Features:
        - Missing value handling (fill, drop, impute)
        - Deduplication with configurable strategies
        - Date/time standardization
        - Derived metric calculation
        
    Example:
        cleaner = DataCleaner()
        cleaned_df = cleaner.clean_orders(raw_orders_df)
    """
    
    def __init__(self):
        """Initialize data cleaner with logging."""
        self.logger = ETLLogger("transform")
    
    def handle_missing_values(
        self,
        df: pd.DataFrame,
        strategy: str = "drop",
        columns: Optional[list[str]] = None,
        fill_value: Any = None,
        fill_method: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Handle missing values in a DataFrame.
        
        Args:
            df: Input DataFrame
            strategy: One of 'drop', 'fill', 'interpolate'
            columns: Specific columns to apply strategy (None = all)
            fill_value: Value to fill with if strategy='fill'
            fill_method: Pandas fill method ('ffill', 'bfill')
            
        Returns:
            DataFrame with missing values handled
        """
        df = df.copy()
        target_cols = columns or df.columns.tolist()
        
        original_nulls = df[target_cols].isnull().sum().sum()
        
        if strategy == "drop":
            df = df.dropna(subset=target_cols)
            
        elif strategy == "fill":
            if fill_method:
                df[target_cols] = df[target_cols].fillna(method=fill_method)
            elif fill_value is not None:
                df[target_cols] = df[target_cols].fillna(fill_value)
            else:
                # Fill with appropriate defaults based on dtype
                for col in target_cols:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = df[col].fillna(df[col].median())
                    else:
                        df[col] = df[col].fillna("UNKNOWN")
                        
        elif strategy == "interpolate":
            df[target_cols] = df[target_cols].interpolate(method="linear")
        
        remaining_nulls = df[target_cols].isnull().sum().sum()
        
        self.logger.info(
            "Missing values handled",
            strategy=strategy,
            original_nulls=int(original_nulls),
            remaining_nulls=int(remaining_nulls)
        )
        
        return df
    
    def remove_duplicates(
        self,
        df: pd.DataFrame,
        subset: Optional[list[str]] = None,
        keep: str = "first"
    ) -> pd.DataFrame:
        """
        Remove duplicate rows from DataFrame.
        
        Args:
            df: Input DataFrame
            subset: Columns to consider for duplicates
            keep: 'first', 'last', or False (drop all)
            
        Returns:
            DataFrame with duplicates removed
        """
        original_count = len(df)
        df = df.drop_duplicates(subset=subset, keep=keep)
        removed = original_count - len(df)
        
        self.logger.info(
            "Duplicates removed",
            original_count=original_count,
            removed=removed,
            remaining=len(df)
        )
        
        return df
    
    def standardize_timestamps(
        self,
        df: pd.DataFrame,
        columns: list[str],
        output_format: str = "%Y-%m-%d %H:%M:%S",
        errors: str = "coerce"
    ) -> pd.DataFrame:
        """
        Standardize timestamp columns to a consistent format.
        
        Args:
            df: Input DataFrame
            columns: List of timestamp column names
            output_format: Output datetime format (ISO 8601 by default)
            errors: How to handle parsing errors ('coerce', 'raise', 'ignore')
            
        Returns:
            DataFrame with standardized timestamps
        """
        df = df.copy()
        
        for col in columns:
            if col not in df.columns:
                self.logger.warning(f"Column not found: {col}")
                continue
            
            # Convert to datetime
            df[col] = pd.to_datetime(df[col], errors=errors)
            
            # Ensure consistent timezone (UTC)
            if df[col].dt.tz is not None:
                df[col] = df[col].dt.tz_convert("UTC")
            
            self.logger.debug(f"Standardized timestamp column: {col}")
        
        self.logger.info(
            "Timestamps standardized",
            columns=columns
        )
        
        return df
    
    def normalize_categorical(
        self,
        df: pd.DataFrame,
        columns: list[str],
        case: str = "lower"
    ) -> pd.DataFrame:
        """
        Normalize categorical string columns.
        
        Args:
            df: Input DataFrame
            columns: List of column names to normalize
            case: 'lower', 'upper', or 'title'
            
        Returns:
            DataFrame with normalized categorical values
        """
        df = df.copy()
        
        for col in columns:
            if col not in df.columns:
                continue
                
            if df[col].dtype == "object":
                # Strip whitespace
                df[col] = df[col].str.strip()
                
                # Apply case transformation
                if case == "lower":
                    df[col] = df[col].str.lower()
                elif case == "upper":
                    df[col] = df[col].str.upper()
                elif case == "title":
                    df[col] = df[col].str.title()
        
        self.logger.info(
            "Categorical values normalized",
            columns=columns,
            case=case
        )
        
        return df
    
    def calculate_derived_metrics(
        self,
        df: pd.DataFrame,
        metrics: dict[str, Callable[[pd.DataFrame], pd.Series]]
    ) -> pd.DataFrame:
        """
        Calculate derived metrics from existing columns.
        
        Args:
            df: Input DataFrame
            metrics: Dictionary mapping new column names to calculation functions
            
        Returns:
            DataFrame with new derived columns
            
        Example:
            metrics = {
                "delivery_hours": lambda df: (
                    df["delivered_at"] - df["ordered_at"]
                ).dt.total_seconds() / 3600
            }
        """
        df = df.copy()
        
        for col_name, calc_func in metrics.items():
            try:
                df[col_name] = calc_func(df)
                self.logger.debug(f"Calculated metric: {col_name}")
            except Exception as e:
                self.logger.warning(
                    f"Failed to calculate {col_name}: {e}"
                )
        
        self.logger.info(
            "Derived metrics calculated",
            metrics=list(metrics.keys())
        )
        
        return df


class OrdersCleaner(DataCleaner):
    """
    Specialized cleaner for order/delivery data.
    
    Handles the specific cleaning needs of order timestamps
    and delivery duration calculations.
    """
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Full cleaning pipeline for orders data.
        
        Steps:
        1. Remove exact duplicates
        2. Standardize timestamp columns
        3. Handle missing values in critical fields
        4. Calculate delivery duration
        5. Normalize status values
        
        Args:
            df: Raw orders DataFrame
            
        Returns:
            Cleaned orders DataFrame
        """
        self.logger.info("Starting orders cleaning pipeline")
        
        # Step 1: Remove duplicates
        if "order_id" in df.columns:
            df = self.remove_duplicates(df, subset=["order_id"])
        else:
            df = self.remove_duplicates(df)
        
        # Step 2: Standardize timestamps
        timestamp_cols = [
            col for col in df.columns 
            if "timestamp" in col.lower() or "date" in col.lower()
        ]
        if timestamp_cols:
            df = self.standardize_timestamps(df, timestamp_cols)
        
        # Step 3: Handle missing values
        # For orders, we require order_id and purchase timestamp
        required_cols = [
            col for col in ["order_id", "order_purchase_timestamp"]
            if col in df.columns
        ]
        if required_cols:
            df = self.handle_missing_values(
                df, 
                strategy="drop",
                columns=required_cols
            )
        
        # Step 4: Calculate delivery duration
        if all(col in df.columns for col in [
            "order_purchase_timestamp", 
            "order_delivered_customer_date"
        ]):
            df = self.calculate_derived_metrics(df, {
                "delivery_duration_hours": lambda d: (
                    (d["order_delivered_customer_date"] - d["order_purchase_timestamp"])
                    .dt.total_seconds() / 3600
                ).round(2)
            })
        
        # Step 5: Normalize status
        if "order_status" in df.columns:
            df = self.normalize_categorical(df, ["order_status"], case="lower")
        
        self.logger.info(
            "Orders cleaning complete",
            final_rows=len(df)
        )
        
        return df


class ProductsCleaner(DataCleaner):
    """
    Specialized cleaner for product/catalog data.
    """
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Full cleaning pipeline for products data.
        
        Args:
            df: Raw products DataFrame
            
        Returns:
            Cleaned products DataFrame
        """
        self.logger.info("Starting products cleaning pipeline")
        
        # Remove duplicates by product ID
        if "id" in df.columns:
            df = self.remove_duplicates(df, subset=["id"])
        elif "product_id" in df.columns:
            df = self.remove_duplicates(df, subset=["product_id"])
        
        # Normalize categories
        if "category" in df.columns:
            df = self.normalize_categorical(df, ["category"], case="lower")
        
        # Handle missing prices
        if "price" in df.columns:
            df = self.handle_missing_values(
                df,
                strategy="fill",
                columns=["price"],
                fill_value=0.0
            )
        
        self.logger.info(
            "Products cleaning complete",
            final_rows=len(df)
        )
        
        return df


class OrderItemsCleaner(DataCleaner):
    """
    Specialized cleaner for order line items.
    """
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Full cleaning pipeline for order items data.
        
        Args:
            df: Raw order items DataFrame
            
        Returns:
            Cleaned order items DataFrame
        """
        self.logger.info("Starting order items cleaning pipeline")
        
        # Remove duplicates
        dup_cols = [c for c in ["order_id", "product_id"] if c in df.columns]
        if dup_cols:
            df = self.remove_duplicates(df, subset=dup_cols)
        
        # Handle missing shipping cost
        if "freight_value" in df.columns:
            df = self.handle_missing_values(
                df,
                strategy="fill",
                columns=["freight_value"],
                fill_value=0.0
            )
        
        if "shipping_cost" in df.columns:
            df = self.handle_missing_values(
                df,
                strategy="fill",
                columns=["shipping_cost"],
                fill_value=0.0
            )
        
        # Calculate shipping cost per item
        if all(c in df.columns for c in ["freight_value", "price"]):
            df = self.calculate_derived_metrics(df, {
                "shipping_cost_ratio": lambda d: (
                    d["freight_value"] / d["price"].replace(0, np.nan)
                ).round(4)
            })
        
        self.logger.info(
            "Order items cleaning complete",
            final_rows=len(df)
        )
        
        return df
