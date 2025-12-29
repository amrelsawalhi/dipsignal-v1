"""
Shared test fixtures and configuration for pytest
"""
import pytest
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture
def sample_crypto_data():
    """Sample cryptocurrency data for testing"""
    return {
        'BTCUSDT': 'BTC',
        'ETHUSDT': 'ETH',
        'SOLUSDT': 'SOL'
    }


@pytest.fixture
def sample_stock_tickers():
    """Sample stock tickers for testing"""
    return ['AAPL', 'GOOGL', 'MSFT', 'TSLA']


@pytest.fixture
def sample_commodity_symbols():
    """Sample commodity symbols for testing"""
    return ['GC=F', 'SI=F', 'CL=F']


@pytest.fixture
def mock_db_connection():
    """Mock database connection"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def sample_price_data():
    """Sample price data for testing"""
    return {
        'timestamp': datetime.now(),
        'price_open': 100.0,
        'price_high': 105.0,
        'price_low': 98.0,
        'price_close': 103.0,
        'volume': 1000000
    }


@pytest.fixture
def sample_fgi_data():
    """Sample Fear & Greed Index data"""
    return {
        'value': 65,
        'value_classification': 'Greed',
        'timestamp': datetime.now().strftime('%Y-%m-%d')
    }


@pytest.fixture
def mock_api_response():
    """Mock API response"""
    def _mock_response(status_code=200, json_data=None):
        mock_resp = Mock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = json_data or {}
        mock_resp.raise_for_status = Mock()
        return mock_resp
    return _mock_response


@pytest.fixture
def test_env_vars(monkeypatch):
    """Set test environment variables"""
    monkeypatch.setenv('DATABASE_URL', 'postgresql://test:test@localhost:5432/test_db')
    monkeypatch.setenv('FRED_API_KEY', 'test_fred_key')
    monkeypatch.setenv('GOOGLE_API_KEY', 'test_google_key')
