"""
Unit tests for collectors/fetch_fgi.py
Tests Fear & Greed Index data collection
"""
import pytest
from unittest.mock import patch, Mock
from datetime import datetime
import pandas as pd


@pytest.mark.unit
class TestFetchFGI:
    """Test suite for Fear & Greed Index collector"""
    
    def test_fetch_fgi_success(self):
        """Test successful FGI data fetch"""
        from src.collectors.fetch_fgi import fetch_fgi
        
        # Mock successful API response
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


@pytest.mark.unit  
class TestDataValidation:
    """Test suite for data validation"""
    
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
