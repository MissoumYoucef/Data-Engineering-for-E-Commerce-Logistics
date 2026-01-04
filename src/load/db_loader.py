"""
Database Loader Module
======================

Loads cleaned data into SQL database with upsert logic,
batch processing, and transaction management.

Supports:
- SQLite (local development)
- PostgreSQL (production)
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..utils.config import config
from ..utils.logger import ETLLogger


class DatabaseLoader:
    """
    SQL database loader with upsert capabilities.
    
    Implements the Load phase of the ETL pipeline with:
    - Configurable batch sizes for memory efficiency
    - Upsert logic for idempotent loads
    - Transaction management for data integrity
    - Support for SQLite (dev) and PostgreSQL (prod)
    
    Similar to how fleet data is loaded into analytics databases.
    
    Example:
        loader = DatabaseLoader()
        loader.initialize_schema()
        loader.load_orders(orders_df)
    """
    
    def __init__(self, db_type: Optional[str] = None):
        """
        Initialize database loader.
        
        Args:
            db_type: 'sqlite' or 'postgresql' (uses config if not provided)
        """
        self.logger = ETLLogger("load")
        self.db_type = db_type or config.database.get("type", "sqlite")
        self.batch_size = config.load.get("batch_size", 1000)
        self.upsert_enabled = config.load.get("upsert_enabled", True)
        
        self.engine = self._create_engine()
        self.metadata = MetaData()
    
    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine based on configuration."""
        if self.db_type == "sqlite":
            db_path = config.database.get("sqlite", {}).get(
                "path", "data/logiflow.db"
            )
            # Ensure directory exists
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            connection_string = f"sqlite:///{db_path}"
            
        elif self.db_type == "postgresql":
            pg_config = config.database.get("postgresql", {})
            connection_string = (
                f"postgresql://{pg_config.get('username')}:"
                f"{pg_config.get('password')}@"
                f"{pg_config.get('host', 'localhost')}:"
                f"{pg_config.get('port', 5432)}/"
                f"{pg_config.get('database', 'logiflow')}"
            )
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
        
        self.logger.info(f"Creating database engine", db_type=self.db_type)
        return create_engine(connection_string)
    
    def initialize_schema(self, drop_existing: bool = False) -> None:
        """
        Initialize database schema.
        
        Args:
            drop_existing: If True, drop existing tables first (DANGER!)
        """
        self.logger.info("Initializing database schema")
        
        # Define tables as individual statements for reliable execution
        tables = [
            """
            CREATE TABLE IF NOT EXISTS customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                customer_unique_id VARCHAR(50),
                customer_city VARCHAR(100),
                customer_state VARCHAR(10),
                customer_zip_code VARCHAR(20),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(200),
                phone VARCHAR(50),
                city VARCHAR(100),
                street VARCHAR(200),
                zipcode VARCHAR(20),
                lat VARCHAR(50),
                lng VARCHAR(50),
                extracted_at TIMESTAMP,
                source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sellers (
                seller_id VARCHAR(50) PRIMARY KEY,
                seller_city VARCHAR(100),
                seller_state VARCHAR(10),
                seller_zip_code VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(500),
                description TEXT,
                category VARCHAR(100),
                price DECIMAL(10, 2),
                image VARCHAR(500),
                rating_rate DECIMAL(3, 2),
                rating_count INTEGER,
                source VARCHAR(50),
                extracted_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50),
                order_status VARCHAR(30),
                order_date TIMESTAMP,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP,
                delivery_duration_hours DECIMAL(10, 2),
                source VARCHAR(50),
                extracted_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id VARCHAR(50) NOT NULL,
                product_id VARCHAR(50),
                seller_id VARCHAR(50),
                order_item_id INTEGER,
                quantity INTEGER DEFAULT 1,
                price DECIMAL(10, 2),
                freight_value DECIMAL(10, 2),
                shipping_cost DECIMAL(10, 2),
                shipping_cost_ratio DECIMAL(6, 4),
                shipping_limit_date TIMESTAMP,
                source VARCHAR(50),
                extracted_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS etl_run_log (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                table_name VARCHAR(50) NOT NULL,
                source VARCHAR(50),
                rows_extracted INTEGER,
                rows_transformed INTEGER,
                rows_loaded INTEGER,
                validation_passed BOOLEAN,
                validation_errors TEXT,
                duration_seconds DECIMAL(10, 2),
                status VARCHAR(20) DEFAULT 'running'
            )
            """
        ]
        
        with self.engine.begin() as conn:
            if drop_existing:
                for table in ["order_items", "orders", "products", "sellers", "customers", "etl_run_log"]:
                    try:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                    except SQLAlchemyError:
                        pass
            
            for table_sql in tables:
                try:
                    conn.execute(text(table_sql))
                except SQLAlchemyError as e:
                    if "already exists" not in str(e).lower():
                        self.logger.warning(f"Table creation warning: {e}")
        
        self.logger.info("Schema initialization complete")
    
    def _get_primary_key(self, table_name: str) -> Optional[str]:
        """Get the primary key column name for a table."""
        pk_mapping = {
            "customers": "customer_id",
            "sellers": "seller_id", 
            "products": "product_id",
            "orders": "order_id",
            "order_items": "id"
        }
        return pk_mapping.get(table_name)
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        upsert_key: Optional[str] = None
    ) -> int:
        """
        Load a DataFrame into a database table.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: 'append', 'replace', or 'fail'
            upsert_key: Column for upsert logic (None = simple insert)
            
        Returns:
            Number of rows loaded
        """
        if df.empty:
            self.logger.warning(f"Empty DataFrame, skipping load", table=table_name)
            return 0
        
        df = df.copy()
        
        # Add timestamp metadata
        df["updated_at"] = datetime.now()
        if "created_at" not in df.columns:
            df["created_at"] = datetime.now()
        
        # Get existing columns in the table
        try:
            inspector = inspect(self.engine)
            existing_columns = [c["name"] for c in inspector.get_columns(table_name)]
            # Filter DataFrame to only include existing columns
            df = df[[c for c in df.columns if c in existing_columns]]
        except Exception:
            # Table might not exist yet
            pass
        
        self.logger.info(
            f"Loading data",
            table=table_name,
            rows=len(df),
            columns=len(df.columns)
        )
        
        rows_loaded = 0
        
        if self.upsert_enabled and upsert_key and upsert_key in df.columns:
            # Check if table has data - use simple insert for empty tables
            try:
                count_result = pd.read_sql(
                    f"SELECT COUNT(*) as cnt FROM {table_name}", 
                    self.engine
                )
                table_has_data = count_result.iloc[0]['cnt'] > 0
            except Exception:
                table_has_data = False
            
            if table_has_data:
                rows_loaded = self._upsert_data(df, table_name, upsert_key)
            else:
                # Simple insert for empty table
                df.to_sql(table_name, self.engine, if_exists="append", index=False)
                rows_loaded = len(df)
        else:
            # Simple batch insert
            for start in range(0, len(df), self.batch_size):
                batch = df.iloc[start:start + self.batch_size]
                try:
                    batch.to_sql(
                        table_name,
                        self.engine,
                        if_exists=if_exists,
                        index=False
                    )
                    rows_loaded += len(batch)
                except SQLAlchemyError as e:
                    self.logger.error(
                        f"Batch insert failed",
                        table=table_name,
                        batch_start=start,
                        error=str(e)
                    )
        
        self.logger.info(
            f"Load complete",
            table=table_name,
            rows_loaded=rows_loaded
        )
        
        return rows_loaded
    
    def _upsert_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_column: str
    ) -> int:
        """
        Perform upsert (INSERT ON CONFLICT UPDATE) operation.
        
        Args:
            df: DataFrame to upsert
            table_name: Target table
            key_column: Primary/unique key column
            
        Returns:
            Number of rows affected
        """
        if key_column not in df.columns:
            self.logger.warning(
                f"Upsert key not in DataFrame, falling back to insert",
                key=key_column
            )
            df.to_sql(table_name, self.engine, if_exists="append", index=False)
            return len(df)
        
        rows_affected = 0
        
        with self.engine.begin() as conn:
            for start in range(0, len(df), self.batch_size):
                batch = df.iloc[start:start + self.batch_size]
                
                for _, row in batch.iterrows():
                    columns = [c for c in row.index if pd.notna(row[c])]
                    values = {c: row[c] for c in columns}
                    
                    if self.db_type == "sqlite":
                        # SQLite UPSERT syntax
                        placeholders = ", ".join([f":{c}" for c in columns])
                        col_list = ", ".join(columns)
                        update_clause = ", ".join([
                            f"{c} = :{c}" for c in columns if c != key_column
                        ])
                        
                        sql = f"""
                            INSERT INTO {table_name} ({col_list})
                            VALUES ({placeholders})
                            ON CONFLICT({key_column}) DO UPDATE SET
                            {update_clause}
                        """
                    else:
                        # PostgreSQL UPSERT syntax
                        placeholders = ", ".join([f":{c}" for c in columns])
                        col_list = ", ".join(columns)
                        update_clause = ", ".join([
                            f"{c} = EXCLUDED.{c}" for c in columns if c != key_column
                        ])
                        
                        sql = f"""
                            INSERT INTO {table_name} ({col_list})
                            VALUES ({placeholders})
                            ON CONFLICT ({key_column}) DO UPDATE SET
                            {update_clause}
                        """
                    
                    try:
                        conn.execute(text(sql), values)
                        rows_affected += 1
                    except SQLAlchemyError as e:
                        self.logger.debug(f"Upsert row failed: {e}")
        
        return rows_affected
    
    def load_customers(self, df: pd.DataFrame) -> int:
        """Load customers data with upsert."""
        return self.load_dataframe(df, "customers", upsert_key="customer_id")
    
    def load_sellers(self, df: pd.DataFrame) -> int:
        """Load sellers data with upsert."""
        return self.load_dataframe(df, "sellers", upsert_key="seller_id")
    
    def load_products(self, df: pd.DataFrame) -> int:
        """Load products data with upsert."""
        # Rename columns if needed (API uses 'id', schema uses 'product_id')
        df = df.copy()
        if "id" in df.columns and "product_id" not in df.columns:
            df = df.rename(columns={"id": "product_id"})
        return self.load_dataframe(df, "products", upsert_key="product_id")
    
    def load_orders(self, df: pd.DataFrame) -> int:
        """Load orders data with upsert."""
        return self.load_dataframe(df, "orders", upsert_key="order_id")
    
    def load_order_items(self, df: pd.DataFrame) -> int:
        """Load order items data (no upsert, append only)."""
        return self.load_dataframe(df, "order_items", if_exists="append")
    
    def query(self, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            Query results as DataFrame
        """
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    
    def get_table_counts(self) -> dict[str, int]:
        """Get row counts for all tables."""
        tables = ["customers", "sellers", "products", "orders", "order_items"]
        counts = {}
        
        for table in tables:
            try:
                result = self.query(f"SELECT COUNT(*) as count FROM {table}")
                counts[table] = int(result.iloc[0]["count"])
            except Exception:
                counts[table] = 0
        
        return counts
    
    def log_etl_run(
        self,
        table_name: str,
        source: str,
        rows_extracted: int,
        rows_transformed: int,
        rows_loaded: int,
        validation_passed: bool,
        validation_errors: Optional[str] = None,
        duration_seconds: Optional[float] = None,
        status: str = "completed"
    ) -> None:
        """Log ETL run metrics for monitoring."""
        log_data = pd.DataFrame([{
            "table_name": table_name,
            "source": source,
            "rows_extracted": rows_extracted,
            "rows_transformed": rows_transformed,
            "rows_loaded": rows_loaded,
            "validation_passed": validation_passed,
            "validation_errors": validation_errors,
            "duration_seconds": duration_seconds,
            "status": status
        }])
        
        log_data.to_sql("etl_run_log", self.engine, if_exists="append", index=False)
        
        self.logger.info(
            "ETL run logged",
            table=table_name,
            status=status
        )
