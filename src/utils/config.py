"""
Configuration Management Module
================================

Loads configuration from YAML files and environment variables.
Provides a centralized Config object for all ETL components.
"""

import os
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv


class Config:
    """
    Centralized configuration management for the ETL pipeline.
    
    Loads settings from config.yaml and supports environment variable
    substitution for sensitive values like database credentials.
    
    Usage:
        config = Config()
        db_host = config.get("database.postgresql.host")
        api_url = config.api.fake_store.base_url
    """
    
    _instance: Optional["Config"] = None
    _config: dict = {}
    
    def __new__(cls) -> "Config":
        """Singleton pattern to ensure one config instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self) -> None:
        """Load configuration from YAML file and environment variables."""
        # Load environment variables from .env file
        load_dotenv()
        
        # Find config file path
        config_path = self._find_config_file()
        
        if config_path and config_path.exists():
            with open(config_path, "r") as f:
                self._config = yaml.safe_load(f) or {}
        
        # Substitute environment variables
        self._substitute_env_vars(self._config)
    
    def _find_config_file(self) -> Optional[Path]:
        """Find the config.yaml file relative to the project root."""
        # Try multiple possible locations
        possible_paths = [
            Path(__file__).parent.parent.parent / "config" / "config.yaml",
            Path.cwd() / "config" / "config.yaml",
            Path.cwd() / "config.yaml",
        ]
        
        for path in possible_paths:
            if path.exists():
                return path
        
        return None
    
    def _substitute_env_vars(self, config: dict) -> None:
        """Recursively substitute ${ENV_VAR} patterns with environment values."""
        for key, value in config.items():
            if isinstance(value, dict):
                self._substitute_env_vars(value)
            elif isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                env_var = value[2:-1]
                config[key] = os.getenv(env_var, value)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path, e.g., "database.postgresql.host"
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key_path.split(".")
        value = self._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    @property
    def api(self) -> dict:
        """Get API configuration section."""
        return self._config.get("api", {})
    
    @api.setter
    def api(self, value: dict):
        self._config["api"] = value
    
    @property
    def database(self) -> dict:
        """Get database configuration section."""
        return self._config.get("database", {})
    
    @database.setter
    def database(self, value: dict):
        self._config["database"] = value
    
    @property
    def paths(self) -> dict:
        """Get paths configuration section."""
        return self._config.get("paths", {})
    
    @paths.setter
    def paths(self, value: dict):
        self._config["paths"] = value
    
    @property
    def transform(self) -> dict:
        """Get transform configuration section."""
        return self._config.get("transform", {})
    
    @transform.setter
    def transform(self, value: dict):
        self._config["transform"] = value
    
    @property
    def load(self) -> dict:
        """Get load configuration section."""
        return self._config.get("load", {})
    
    @load.setter
    def load(self, value: dict):
        self._config["load"] = value
    
    @property
    def logging(self) -> dict:
        """Get logging configuration section."""
        return self._config.get("logging", {})
    
    @logging.setter
    def logging(self, value: dict):
        self._config["logging"] = value
    
    def __repr__(self) -> str:
        return f"Config(keys={list(self._config.keys())})"


# Create a global config instance for easy import
config = Config()
