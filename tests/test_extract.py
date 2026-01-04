"""Tests for the Extract layer modules."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import requests


class TestAPIConnector:
    """Test suite for the API Connector."""
    
    @patch("src.extract.api_connector.requests.Session")
    def test_fetch_products_returns_dataframe(self, mock_session):
        """Test that fetch_products returns a DataFrame with expected columns."""
        from src.extract.api_connector import APIConnector
        
        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "id": 1,
                "title": "Test Product",
                "price": 29.99,
                "description": "A test product",
                "category": "test",
                "image": "http://example.com/img.jpg",
                "rating": {"rate": 4.5, "count": 100}
            }
        ]
        mock_response.raise_for_status = Mock()
        mock_session.return_value.get.return_value = mock_response
        
        connector = APIConnector()
        connector.session = mock_session.return_value
        
        df = connector.fetch_products()
        
        assert isinstance(df, pd.DataFrame)
        assert "id" in df.columns
        assert "price" in df.columns
        assert "rating_rate" in df.columns
        assert len(df) == 1
    
    @patch("src.extract.api_connector.requests.Session")
    def test_fetch_carts_expands_products(self, mock_session):
        """Test that fetch_carts expands cart products into rows."""
        from src.extract.api_connector import APIConnector
        
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "id": 1,
                "userId": 1,
                "date": "2024-01-01",
                "products": [
                    {"productId": 1, "quantity": 2},
                    {"productId": 2, "quantity": 1}
                ]
            }
        ]
        mock_response.raise_for_status = Mock()
        mock_session.return_value.get.return_value = mock_response
        
        connector = APIConnector()
        connector.session = mock_session.return_value
        
        df = connector.fetch_carts()
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2  # 2 line items from 1 cart
        assert "order_id" in df.columns
        assert "product_id" in df.columns
    
    def test_api_connector_initialization(self):
        """Test APIConnector initializes with correct config."""
        from src.extract.api_connector import APIConnector
        
        connector = APIConnector()
        
        assert connector.base_url == "https://fakestoreapi.com"
        assert connector.retry_attempts >= 1
        assert connector.timeout > 0


class TestCSVLoader:
    """Test suite for the CSV Loader."""
    
    def test_csv_loader_initialization(self):
        """Test CSVLoader initializes with correct paths."""
        from src.extract.csv_loader import CSVLoader
        
        loader = CSVLoader()
        
        assert loader.data_path is not None
    
    def test_profile_data_returns_expected_structure(self):
        """Test profile_data returns comprehensive statistics."""
        from src.extract.csv_loader import CSVLoader
        
        df = pd.DataFrame({
            "id": [1, 2, 3, None],
            "name": ["a", "b", "c", "d"],
            "value": [10.0, 20.0, 30.0, 40.0]
        })
        
        profile = CSVLoader.profile_data(df)
        
        assert profile["row_count"] == 4
        assert profile["column_count"] == 3
        assert "columns" in profile
        assert "null_count" in profile["columns"]["id"]
        assert profile["columns"]["id"]["null_count"] == 1
    
    def test_csv_loader_file_not_found(self):
        """Test CSVLoader raises error for missing files."""
        from src.extract.csv_loader import CSVLoader
        
        loader = CSVLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_csv("/nonexistent/file.csv")
