"""Tests for the Transform layer modules."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime


class TestDataCleaner:
    """Test suite for the DataCleaner class."""
    
    def test_handle_missing_values_drop(self):
        """Test dropping rows with missing values."""
        from src.transform.cleaners import DataCleaner
        
        df = pd.DataFrame({
            "id": [1, 2, 3, None],
            "name": ["a", "b", None, "d"]
        })
        
        cleaner = DataCleaner()
        result = cleaner.handle_missing_values(df, strategy="drop")
        
        assert len(result) == 2  # Only rows without nulls
    
    def test_handle_missing_values_fill(self):
        """Test filling missing values with defaults."""
        from src.transform.cleaners import DataCleaner
        
        df = pd.DataFrame({
            "value": [1.0, 2.0, None, 4.0]
        })
        
        cleaner = DataCleaner()
        result = cleaner.handle_missing_values(
            df, 
            strategy="fill",
            columns=["value"]
        )
        
        assert result["value"].isnull().sum() == 0
    
    def test_remove_duplicates(self):
        """Test duplicate removal."""
        from src.transform.cleaners import DataCleaner
        
        df = pd.DataFrame({
            "id": [1, 1, 2, 3],
            "value": [10, 10, 20, 30]
        })
        
        cleaner = DataCleaner()
        result = cleaner.remove_duplicates(df, subset=["id"])
        
        assert len(result) == 3
    
    def test_normalize_categorical(self):
        """Test categorical normalization."""
        from src.transform.cleaners import DataCleaner
        
        df = pd.DataFrame({
            "status": ["  ACTIVE ", "active", "ACTIVE", "inactive"]
        })
        
        cleaner = DataCleaner()
        result = cleaner.normalize_categorical(df, ["status"], case="lower")
        
        assert all(s == s.lower() for s in result["status"])
        assert result["status"].iloc[0] == "active"


class TestOrdersCleaner:
    """Test suite for OrdersCleaner."""
    
    def test_clean_orders_removes_duplicates(self):
        """Test orders cleaning removes duplicate order_ids."""
        from src.transform.cleaners import OrdersCleaner
        
        df = pd.DataFrame({
            "order_id": ["ord1", "ord1", "ord2"],
            "order_purchase_timestamp": ["2024-01-01", "2024-01-01", "2024-01-02"],
            "order_status": ["delivered", "delivered", "pending"]
        })
        
        cleaner = OrdersCleaner()
        result = cleaner.clean(df)
        
        assert len(result) == 2
        assert result["order_id"].nunique() == 2
    
    def test_clean_orders_calculates_duration(self):
        """Test delivery duration calculation."""
        from src.transform.cleaners import OrdersCleaner
        
        df = pd.DataFrame({
            "order_id": ["ord1"],
            "order_purchase_timestamp": ["2024-01-01 10:00:00"],
            "order_delivered_customer_date": ["2024-01-02 10:00:00"],
            "order_status": ["delivered"]
        })
        
        cleaner = OrdersCleaner()
        result = cleaner.clean(df)
        
        assert "delivery_duration_hours" in result.columns
        assert result["delivery_duration_hours"].iloc[0] == 24.0


class TestValidators:
    """Test suite for data validators."""
    
    def test_null_check_passes(self):
        """Test null check passes for columns without nulls."""
        from src.transform.validators import DataValidator, ValidationSeverity
        
        df = pd.DataFrame({
            "order_id": ["1", "2", "3"]
        })
        
        validator = DataValidator("test")
        validator.add_null_check("order_id", max_null_pct=0.0)
        report = validator.validate(df)
        
        assert report.passed
    
    def test_null_check_fails(self):
        """Test null check fails when threshold exceeded."""
        from src.transform.validators import DataValidator, ValidationSeverity
        
        df = pd.DataFrame({
            "order_id": ["1", None, "3"]  # 33% null
        })
        
        validator = DataValidator("test")
        validator.add_null_check("order_id", max_null_pct=0.1)
        report = validator.validate(df)
        
        assert not report.passed
    
    def test_unique_check_passes(self):
        """Test uniqueness check passes for unique values."""
        from src.transform.validators import DataValidator
        
        df = pd.DataFrame({
            "id": [1, 2, 3]
        })
        
        validator = DataValidator("test")
        validator.add_unique_check(["id"])
        report = validator.validate(df)
        
        assert report.passed
    
    def test_unique_check_fails(self):
        """Test uniqueness check fails for duplicate values."""
        from src.transform.validators import DataValidator
        
        df = pd.DataFrame({
            "id": [1, 1, 2]
        })
        
        validator = DataValidator("test")
        validator.add_unique_check(["id"])
        report = validator.validate(df)
        
        assert not report.passed
    
    def test_range_check(self):
        """Test numeric range validation."""
        from src.transform.validators import DataValidator
        
        df = pd.DataFrame({
            "price": [10.0, 20.0, -5.0]  # Negative price invalid
        })
        
        validator = DataValidator("test")
        validator.add_range_check("price", min_value=0.0)
        report = validator.validate(df)
        
        assert not report.passed
