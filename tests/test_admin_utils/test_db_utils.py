"""
Simplified unit tests for admin_utils/db_utils.py
Tests database utility functions with proper mocking
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd


@pytest.mark.unit
class TestDatabaseUtils:
    """Test suite for database utility functions"""
    
    def test_test_connection_success(self):
        """Test successful database connection"""
        from admin_utils.db_utils import test_connection
        
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        with patch('admin_utils.db_utils.DBManager.get_engine', return_value=mock_engine):
            status, message = test_connection()
            assert status is True
            assert "successful" in message.lower()
    
    def test_test_connection_failure(self):
        """Test database connection failure"""
        from admin_utils.db_utils import test_connection
        
        with patch('admin_utils.db_utils.DBManager.get_engine', side_effect=Exception("Connection failed")):
            status, message = test_connection()
            assert status is False
            assert len(message) > 0
    
    def test_get_table_row_count(self):
        """Test getting table row count"""
        from admin_utils.db_utils import get_table_row_count
        
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (100,)
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        with patch('admin_utils.db_utils.DBManager.get_engine', return_value=mock_engine):
            count = get_table_row_count("dim_assets")
            assert count == 100
    
    def test_execute_query_success(self):
        """Test successful query execution"""
        from admin_utils.db_utils import execute_query
        
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [(1, 'test'), (2, 'test2')]
        mock_result.keys.return_value = ['id', 'name']
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        with patch('admin_utils.db_utils.DBManager.get_engine', return_value=mock_engine):
            df, error = execute_query("SELECT * FROM test_table")
            assert error is None
            assert df is not None
            assert len(df) == 2
    
    def test_execute_query_failure(self):
        """Test query execution failure"""
        from admin_utils.db_utils import execute_query
        
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Query failed")
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        with patch('admin_utils.db_utils.DBManager.get_engine', return_value=mock_engine):
            df, error = execute_query("SELECT * FROM invalid_table")
            assert df is None
            assert error is not None
    
    def test_get_recent_news_count(self):
        """Test getting recent news count"""
        from admin_utils.db_utils import get_recent_news_count
        
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (42,)
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        with patch('admin_utils.db_utils.DBManager.get_engine', return_value=mock_engine):
            count = get_recent_news_count(days=7)
            assert count == 42
    
    def test_get_all_tables(self):
        """Test getting all tables"""
        from admin_utils.db_utils import get_all_tables
        
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [('dim_assets',), ('fact_prices',)]
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        with patch('admin_utils.db_utils.DBManager.get_engine', return_value=mock_engine):
            tables = get_all_tables()
            assert len(tables) == 2
            assert 'dim_assets' in tables
