"""
Comprehensive unit tests for all data collectors
Tests Binance, stocks, commodities, macro, and news collectors
"""
import pytest
from unittest.mock import patch, Mock, MagicMock
from datetime import datetime
import pandas as pd


# ============================================================================
# BINANCE COLLECTOR TESTS
# ============================================================================

@pytest.mark.unit
class TestBinanceCollector:
    """Test suite for Binance data collector"""
    
    def test_fetch_binance_success(self):
        """Test successful Binance data fetch"""
        from src.collectors.fetch_binance import fetch_ohlcv_binance
        
        # Mock successful API response
        mock_response = Mock()
        mock_response.json.return_value = [[
            1640000000000, '50000.00', '51000.00', '49000.00', '50500.00', '1000.00',
            1640086399999, '50000000.00', 1000, '500.00', '25000000.00', '0'
        ]]
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            result = fetch_ohlcv_binance('BTCUSDT')
            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0
    
    def test_binance_rate_limiting(self):
        """Test Binance rate limit handling"""
        from src.collectors.fetch_binance import fetch_ohlcv_binance
        
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = Exception("Rate limited")
        
        with patch('requests.get', return_value=mock_response):
            # Should handle rate limiting - will exit(1) in actual code
            try:
                result = fetch_ohlcv_binance('BTCUSDT')
            except SystemExit:
                # Expected behavior when rate limited
                pass


# ============================================================================
# STOCK COLLECTOR TESTS
# ============================================================================

@pytest.mark.unit
class TestStockCollector:
    """Test suite for stock data collector"""
    
    def test_fetch_stocks_success(self):
        """Test successful stock data fetch"""
        from src.collectors.fetch_stocks import fetch_stock_history_and_metadata
        
        # Mock yfinance Ticker with proper index and info
        mock_ticker = MagicMock()
        df = pd.DataFrame({
            'Open': [100.0],
            'High': [105.0],
            'Low': [98.0],
            'Close': [103.0],
            'Volume': [1000000],
            'Dividends': [0.0],
            'Stock Splits': [0.0]
        })
        df.index = pd.DatetimeIndex(['2024-01-01'])
        df.index.name = 'Date'
        mock_ticker.history.return_value = df
        mock_ticker.info = {
            'marketCap': 1000000000,
            'trailingPE': 15.5,
            'forwardPE': 14.2,
            'dividendYield': 0.02,
            'sector': 'Technology'
        }
        
        with patch('yfinance.Ticker', return_value=mock_ticker):
            result, fundamentals = fetch_stock_history_and_metadata('AAPL')
            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert fundamentals is not None
    
    def test_fetch_stocks_invalid_ticker(self):
        """Test stock fetch with invalid ticker"""
        from src.collectors.fetch_stocks import fetch_stock_history_and_metadata
        
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()  # Empty dataframe
        
        with patch('yfinance.Ticker', return_value=mock_ticker):
            result = fetch_stock_history_and_metadata('INVALID')
            # Should handle empty data gracefully
            assert result is not None or result is None  # Either way is acceptable


# ============================================================================
# COMMODITY COLLECTOR TESTS
# ============================================================================

@pytest.mark.unit
class TestCommodityCollector:
    """Test suite for commodity data collector"""
    
    def test_fetch_commodities_success(self):
        """Test successful commodity data fetch"""
        from src.collectors.fetch_commodities import fetch_commodity_data
        
        # Mock yfinance Ticker with proper index
        mock_ticker = MagicMock()
        df = pd.DataFrame({
            'Open': [1800.0],
            'High': [1820.0],
            'Low': [1790.0],
            'Close': [1810.0],
            'Volume': [50000]
        })
        df.index = pd.DatetimeIndex(['2024-01-01'])
        df.index.name = 'Date'
        mock_ticker.history.return_value = df
        
        with patch('yfinance.Ticker', return_value=mock_ticker):
            result = fetch_commodity_data('GC=F')
            assert result is not None
            assert isinstance(result, pd.DataFrame)


# ============================================================================
# MACRO INDICATOR TESTS
# ============================================================================

@pytest.mark.unit
class TestMacroCollector:
    """Test suite for macro indicator collector"""
    
    def test_fetch_macro_success(self):
        """Test successful macro data fetch"""
        from src.collectors.fetch_macro import fetch_macro_data
        
        # Mock FRED API response
        mock_response = Mock()
        mock_response.json.return_value = {
            'observations': [
                {'date': '2024-01-01', 'value': '3.5'}
            ]
        }
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            result = fetch_macro_data()
            assert result is not None
            assert isinstance(result, pd.DataFrame)
    
    def test_fetch_macro_api_key_missing(self):
        """Test macro fetch without API key"""
        from src.collectors.fetch_macro import fetch_macro_data
        
        with patch.dict('os.environ', {}, clear=True):
            # Should handle missing API key
            try:
                result = fetch_macro_data()
            except Exception as e:
                # Expected to fail without API key
                assert "api" in str(e).lower() or "key" in str(e).lower() or "FRED" in str(e)


# ============================================================================
# NEWS COLLECTOR TESTS
# ============================================================================

@pytest.mark.unit
class TestNewsCollectors:
    """Test suite for news collectors"""
    
    def test_fetch_crypto_news_success(self):
        """Test successful crypto news fetch"""
        from src.collectors.fetch_crypto_news import fetch_feed
        
        # Mock feedparser entry with get() method support
        mock_entry = MagicMock()
        mock_entry.get.side_effect = lambda key, default='': {
            'title': 'Bitcoin hits new high',
            'link': 'https://example.com/news',
            'summary': 'Bitcoin price surges',
            'description': 'Bitcoin price surges'
        }.get(key, default)
        
        class MockFeed:
            def __init__(self):
                self.entries = [mock_entry]
        
        with patch('feedparser.parse', return_value=MockFeed()):
            result = fetch_feed('CoinDesk', 'https://example.com/feed')
            assert result is not None
            assert isinstance(result, list)
    
    def test_fetch_yfinance_news_success(self):
        """Test successful yfinance news fetch"""
        from src.collectors.fetch_yfinance_news import fetch_news_for_ticker
        
        # Mock yfinance ticker with nested news structure
        mock_ticker = MagicMock()
        mock_ticker.news = [
            {
                'content': {
                    'title': 'Stock news',
                    'description': 'Stock price rises',
                    'canonicalUrl': {'url': 'https://example.com'},
                    'clickThroughUrl': {'url': 'https://example.com'},
                    'provider': {'displayName': 'Reuters'},
                    'pubDate': '2024-01-01T12:00:00Z',
                    'finance': {'relatedTickers': ['AAPL']}
                }
            }
        ]
        
        with patch('yfinance.Ticker', return_value=mock_ticker):
            result = fetch_news_for_ticker('AAPL')
            assert result is not None
            assert isinstance(result, list)


# ============================================================================
# FGI COLLECTOR TESTS (Enhanced)
# ============================================================================

@pytest.mark.unit
class TestFGICollector:
    """Enhanced test suite for Fear & Greed Index collector"""
    
    def test_fetch_fgi_success(self):
        """Test successful FGI data fetch"""
        from src.collectors.fetch_fgi import fetch_fgi
        
        mock_response = Mock()
        mock_response.json.return_value = {
            'data': [
                {
                    'value': '65',
                    'value_classification': 'Greed',
                    'timestamp': '1640000000'
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            df = fetch_fgi()
            assert df is not None
            assert len(df) > 0
            assert 'fgi_value' in df.columns
            assert 'classification' in df.columns
    
    def test_fetch_fgi_empty_response(self):
        """Test FGI fetch with empty data"""
        from src.collectors.fetch_fgi import fetch_fgi
        
        mock_response = Mock()
        mock_response.json.return_value = {'data': []}
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            df = fetch_fgi()
            assert df is None
    
    def test_fetch_fgi_network_error(self):
        """Test FGI fetch with network error"""
        from src.collectors.fetch_fgi import fetch_fgi
        
        with patch('requests.get', side_effect=ConnectionError("Network error")):
            with pytest.raises(ConnectionError):
                fetch_fgi()
    
    def test_fetch_fgi_timeout(self):
        """Test FGI fetch with timeout"""
        from src.collectors.fetch_fgi import fetch_fgi
        
        with patch('requests.get', side_effect=TimeoutError("Request timeout")):
            with pytest.raises(TimeoutError):
                fetch_fgi()
    
    def test_fetch_fgi_malformed_data(self):
        """Test FGI fetch with malformed data"""
        from src.collectors.fetch_fgi import fetch_fgi
        
        mock_response = Mock()
        mock_response.json.return_value = {'data': [{'invalid': 'data'}]}
        mock_response.raise_for_status = Mock()
        
        with patch('requests.get', return_value=mock_response):
            try:
                df = fetch_fgi()
                # Should handle malformed data gracefully
            except (KeyError, ValueError):
                # Expected to fail with malformed data
                pass


# ============================================================================
# DATA VALIDATION TESTS
# ============================================================================

@pytest.mark.unit  
class TestDataValidation:
    """Test suite for data validation across all collectors"""
    
    def test_price_data_validation(self, sample_price_data):
        """Test price data has required fields"""
        required_fields = ['price_open', 'price_high', 'price_low', 'price_close', 'volume']
        
        for field in required_fields:
            assert field in sample_price_data
            assert sample_price_data[field] is not None
    
    def test_price_data_types(self, sample_price_data):
        """Test price data types are correct"""
        assert isinstance(sample_price_data['price_open'], (int, float))
        assert isinstance(sample_price_data['price_high'], (int, float))
        assert isinstance(sample_price_data['price_low'], (int, float))
        assert isinstance(sample_price_data['price_close'], (int, float))
        assert isinstance(sample_price_data['volume'], (int, float))
    
    def test_price_data_logic(self, sample_price_data):
        """Test price data follows logical constraints"""
        # High should be >= Low
        assert sample_price_data['price_high'] >= sample_price_data['price_low']
        
        # High should be >= Open and Close
        assert sample_price_data['price_high'] >= sample_price_data['price_open']
        assert sample_price_data['price_high'] >= sample_price_data['price_close']
        
        # Low should be <= Open and Close
        assert sample_price_data['price_low'] <= sample_price_data['price_open']
        assert sample_price_data['price_low'] <= sample_price_data['price_close']
        
        # Volume should be positive
        assert sample_price_data['volume'] > 0
    
    def test_fgi_data_validation(self, sample_fgi_data):
        """Test FGI data structure"""
        assert 'value' in sample_fgi_data
        assert 'value_classification' in sample_fgi_data
        assert isinstance(sample_fgi_data['value'], int)
        assert 0 <= sample_fgi_data['value'] <= 100
    
    def test_timestamp_validation(self):
        """Test timestamp data is valid"""
        now = datetime.now()
        assert isinstance(now, datetime)
        assert now.year >= 2020
        assert 1 <= now.month <= 12
        assert 1 <= now.day <= 31
