"""Tests for the Load layer modules."""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os


class TestDatabaseLoader:
    """Test suite for the DatabaseLoader class."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield os.path.join(tmpdir, "test.db")
    
    def test_loader_initialization_sqlite(self, temp_db_path, monkeypatch):
        """Test DatabaseLoader initializes with SQLite."""
        from src.load.db_loader import DatabaseLoader
        
        # Monkeypatch config to use temp path
        monkeypatch.setattr(
            "src.load.db_loader.config.database",
            {"type": "sqlite", "sqlite": {"path": temp_db_path}}
        )
        
        loader = DatabaseLoader(db_type="sqlite")
        
        assert loader.db_type == "sqlite"
        assert loader.engine is not None
    
    def test_load_dataframe_creates_table(self, temp_db_path, monkeypatch):
        """Test loading DataFrame creates table and inserts data."""
        from src.load.db_loader import DatabaseLoader
        
        monkeypatch.setattr(
            "src.load.db_loader.config.database",
            {"type": "sqlite", "sqlite": {"path": temp_db_path}}
        )
        monkeypatch.setattr(
            "src.load.db_loader.config.load",
            {"batch_size": 100, "upsert_enabled": False}
        )
        
        loader = DatabaseLoader(db_type="sqlite")
        
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [10.0, 20.0, 30.0]
        })
        
        rows_loaded = loader.load_dataframe(df, "test_table")
        
        assert rows_loaded == 3
        
        # Verify data was inserted
        result = loader.query("SELECT COUNT(*) as count FROM test_table")
        assert result.iloc[0]["count"] == 3
    
    def test_query_returns_dataframe(self, temp_db_path, monkeypatch):
        """Test query method returns DataFrame."""
        from src.load.db_loader import DatabaseLoader
        
        monkeypatch.setattr(
            "src.load.db_loader.config.database",
            {"type": "sqlite", "sqlite": {"path": temp_db_path}}
        )
        
        loader = DatabaseLoader(db_type="sqlite")
        
        # Create and populate test table
        df = pd.DataFrame({"x": [1, 2, 3]})
        df.to_sql("test", loader.engine, index=False)
        
        result = loader.query("SELECT * FROM test")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3


class TestSchemaIntegration:
    """Integration tests for the database schema."""
    
    @pytest.fixture
    def loader_with_schema(self, tmp_path, monkeypatch):
        """Create a loader with initialized schema."""
        from src.load.db_loader import DatabaseLoader
        
        db_path = str(tmp_path / "test.db")
        
        monkeypatch.setattr(
            "src.load.db_loader.config.database",
            {"type": "sqlite", "sqlite": {"path": db_path}}
        )
        monkeypatch.setattr(
            "src.load.db_loader.config.load",
            {"batch_size": 100, "upsert_enabled": True}
        )
        
        loader = DatabaseLoader(db_type="sqlite")
        loader.initialize_schema()
        
        return loader
    
    def test_schema_creates_tables(self, loader_with_schema):
        """Test that schema initialization creates expected tables."""
        loader = loader_with_schema
        
        # Check tables exist by querying
        tables = ["customers", "sellers", "products", "orders", "order_items"]
        
        for table in tables:
            try:
                result = loader.query(f"SELECT * FROM {table} LIMIT 1")
                assert isinstance(result, pd.DataFrame)
            except Exception as e:
                pytest.fail(f"Table {table} not created: {e}")
    
    def test_load_products(self, loader_with_schema):
        """Test loading products with upsert."""
        loader = loader_with_schema
        
        df = pd.DataFrame({
            "product_id": ["p1", "p2"],
            "title": ["Product 1", "Product 2"],
            "price": [10.0, 20.0],
            "category": ["test", "test"]
        })
        
        count = loader.load_products(df)
        
        assert count >= 0
    
    def test_load_orders(self, loader_with_schema):
        """Test loading orders."""
        loader = loader_with_schema
        
        df = pd.DataFrame({
            "order_id": ["o1", "o2"],
            "order_status": ["delivered", "pending"],
            "order_purchase_timestamp": ["2024-01-01", "2024-01-02"]
        })
        
        count = loader.load_orders(df)
        
        assert count >= 0
