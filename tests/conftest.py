"""Pytest configuration and fixtures."""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(autouse=True)
def reset_config():
    """Reset config singleton between tests."""
    from src.utils.config import Config
    Config._instance = None
    Config._config = {}
    yield
