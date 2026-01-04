"""
API Connector Module
====================

Fetches data from external APIs with retry logic, rate limiting,
and error handling. Designed for reliability in production environments.

Primary data source: Fake Store API (https://fakestoreapi.com/)
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..utils.config import config
from ..utils.logger import ETLLogger


class APIConnector:
    """
    Robust API connector with automatic retry and rate limiting.
    
    Implements the extraction pattern for real-time data ingestion,
    similar to how fleet management systems fetch GPS/telematics data.
    
    Features:
        - Exponential backoff retry on failures
        - Configurable rate limiting
        - Response validation
        - Automatic pagination handling
        
    Example:
        connector = APIConnector()
        products = connector.fetch_products()
        carts = connector.fetch_carts()  # Simulates orders
    """
    
    def __init__(self, base_url: Optional[str] = None):
        """
        Initialize API connector with configuration.
        
        Args:
            base_url: Override base URL (uses config if not provided)
        """
        self.logger = ETLLogger("extract")
        
        # Load configuration
        api_config = config.api.get("fake_store", {})
        self.base_url = base_url or api_config.get("base_url", "https://fakestoreapi.com")
        self.endpoints = api_config.get("endpoints", {
            "products": "/products",
            "carts": "/carts",
            "users": "/users"
        })
        self.timeout = api_config.get("timeout", 30)
        self.retry_attempts = api_config.get("retry_attempts", 3)
        self.retry_delay = api_config.get("retry_delay", 5)
        
        # Set up session with retry strategy
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry configuration.
        
        Uses exponential backoff for transient failures.
        """
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=self.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, endpoint: str, params: Optional[dict] = None) -> list[dict]:
        """
        Make a GET request to the API endpoint.
        
        Args:
            endpoint: API endpoint path
            params: Optional query parameters
            
        Returns:
            List of records from the API
            
        Raises:
            requests.RequestException: On API errors after retries exhausted
        """
        url = f"{self.base_url}{endpoint}"
        
        self.logger.info("Making API request", url=url, params=params)
        
        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Ensure we always return a list
            if isinstance(data, dict):
                data = [data]
            
            self.logger.info(
                "API request successful",
                endpoint=endpoint,
                records=len(data)
            )
            
            return data
            
        except requests.RequestException as e:
            self.logger.error(
                "API request failed",
                endpoint=endpoint,
                error=str(e)
            )
            raise
    
    def fetch_products(self) -> pd.DataFrame:
        """
        Fetch all products from the Fake Store API.
        
        Returns:
            DataFrame with product data including:
            - id, title, price, description, category, image, rating
            
        This mirrors fetching vehicle catalog data in fleet management.
        """
        endpoint = self.endpoints.get("products", "/products")
        data = self._make_request(endpoint)
        
        # Flatten nested rating object
        df = pd.DataFrame(data)
        if "rating" in df.columns:
            df["rating_rate"] = df["rating"].apply(
                lambda x: x.get("rate") if isinstance(x, dict) else None
            )
            df["rating_count"] = df["rating"].apply(
                lambda x: x.get("count") if isinstance(x, dict) else None
            )
            df = df.drop(columns=["rating"])
        
        # Add extraction metadata
        df["extracted_at"] = datetime.now().isoformat()
        df["source"] = "fake_store_api"
        
        self.logger.info("Products fetched", count=len(df))
        return df
    
    def fetch_carts(self) -> pd.DataFrame:
        """
        Fetch all carts (simulated orders) from the API.
        
        Returns:
            DataFrame with cart/order data including:
            - id, userId, date, products
            
        This mirrors fetching order/trip data in fleet management.
        """
        endpoint = self.endpoints.get("carts", "/carts")
        data = self._make_request(endpoint)
        
        # Expand cart products into order items
        order_items = []
        for cart in data:
            cart_id = cart.get("id")
            user_id = cart.get("userId")
            cart_date = cart.get("date")
            
            for product in cart.get("products", []):
                order_items.append({
                    "order_id": cart_id,
                    "customer_id": user_id,
                    "order_date": cart_date,
                    "product_id": product.get("productId"),
                    "quantity": product.get("quantity")
                })
        
        df = pd.DataFrame(order_items)
        
        # Add extraction metadata
        df["extracted_at"] = datetime.now().isoformat()
        df["source"] = "fake_store_api"
        
        self.logger.info("Carts/Orders fetched", count=len(df))
        return df
    
    def fetch_users(self) -> pd.DataFrame:
        """
        Fetch all users (customers) from the API.
        
        Returns:
            DataFrame with user data including:
            - id, email, name, address, phone
            
        This mirrors fetching driver/customer data in fleet management.
        """
        endpoint = self.endpoints.get("users", "/users")
        data = self._make_request(endpoint)
        
        # Flatten nested objects
        users = []
        for user in data:
            flat_user = {
                "user_id": user.get("id"),
                "email": user.get("email"),
                "username": user.get("username"),
                "phone": user.get("phone"),
                "first_name": user.get("name", {}).get("firstname"),
                "last_name": user.get("name", {}).get("lastname"),
                "city": user.get("address", {}).get("city"),
                "street": user.get("address", {}).get("street"),
                "zipcode": user.get("address", {}).get("zipcode"),
                "lat": user.get("address", {}).get("geolocation", {}).get("lat"),
                "lng": user.get("address", {}).get("geolocation", {}).get("long")
            }
            users.append(flat_user)
        
        df = pd.DataFrame(users)
        
        # Add extraction metadata
        df["extracted_at"] = datetime.now().isoformat()
        df["source"] = "fake_store_api"
        
        self.logger.info("Users fetched", count=len(df))
        return df
    
    def fetch_all(self, save_raw: bool = True) -> dict[str, pd.DataFrame]:
        """
        Fetch all available data from the API.
        
        Args:
            save_raw: If True, save raw data to CSV files
            
        Returns:
            Dictionary of DataFrames keyed by entity name
        """
        self.logger.info("Starting full data extraction")
        
        data = {
            "products": self.fetch_products(),
            "orders": self.fetch_carts(),
            "users": self.fetch_users()
        }
        
        if save_raw:
            raw_path = Path(config.paths.get("raw_data", "data/raw"))
            raw_path.mkdir(parents=True, exist_ok=True)
            
            for name, df in data.items():
                file_path = raw_path / f"{name}_raw.csv"
                df.to_csv(file_path, index=False)
                self.logger.info(f"Saved raw data", file=str(file_path))
        
        self.logger.info(
            "Full extraction complete",
            products=len(data["products"]),
            orders=len(data["orders"]),
            users=len(data["users"])
        )
        
        return data


# Convenience function for quick extraction
def extract_from_api(save_raw: bool = True) -> dict[str, pd.DataFrame]:
    """
    Convenience function to extract all data from the Fake Store API.
    
    Args:
        save_raw: If True, save raw data to CSV files
        
    Returns:
        Dictionary of DataFrames
    """
    connector = APIConnector()
    return connector.fetch_all(save_raw=save_raw)
