"""
Base abstract class for storage backends in LeviafanDL.

Defines the common interface that all storage implementations must implement.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import pandas as pd


class BaseStorage(ABC):
    """
    Abstract base class for financial data storage backends.
    
    All storage implementations (local, Google Drive, FTP) must inherit
    from this class and implement the required methods.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the storage backend with configuration.
        
        Args:
            config: Configuration dictionary from ConfigManager
        """
        self.config = config
        self.storage_type = config.get('type', 'unknown')
    
    @abstractmethod
    def save(self, df: pd.DataFrame, symbol: str, timeframe: str, 
             data_type: str = 'bars', source: str = 'unknown') -> bool:
        """
        Save DataFrame to storage.
        
        Args:
            df: DataFrame to save
            symbol: Trading symbol (e.g., 'EURUSD', 'BTCUSDT')
            timeframe: Timeframe (e.g., '1m', '1h', '1d', 'tick')
            data_type: Type of data ('bars' or 'ticks')
            source: Data source name (e.g., 'dukascopy', 'binance')
            
        Returns:
            True if save successful, False otherwise
        """
        pass
    
    @abstractmethod
    def load(self, symbol: str, timeframe: str, start_date: datetime, 
             end_date: datetime, data_type: str = 'bars') -> pd.DataFrame:
        """
        Load data from storage.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            start_date: Start date for data
            end_date: End date for data
            data_type: Type of data ('bars' or 'ticks')
            
        Returns:
            DataFrame with requested data
        """
        pass
    
    @abstractmethod
    def check_exists(self, symbol: str, timeframe: str, start_date: datetime, 
                    end_date: datetime, data_type: str = 'bars') -> Tuple[bool, Optional[datetime], Optional[datetime]]:
        """
        Check if data exists in storage for given parameters.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            start_date: Requested start date
            end_date: Requested end date
            data_type: Type of data ('bars' or 'ticks')
            
        Returns:
            Tuple of (data_exists, actual_start_date, actual_end_date)
            If data_exists is False, dates will be None
        """
        pass
    
    @abstractmethod
    def get_stored_info(self) -> Dict[str, Any]:
        """
        Get information about stored data.
        
        Returns:
            Dictionary with information about available symbols, timeframes,
            date ranges, and data types
        """
        pass
    
    @abstractmethod
    def delete_data(self, symbol: str, timeframe: str, 
                   data_type: str = 'bars') -> bool:
        """
        Delete stored data for given parameters.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            data_type: Type of data ('bars' or 'ticks')
            
        Returns:
            True if deletion successful, False otherwise
        """
        pass
    
    def get_missing_ranges(self, symbol: str, timeframe: str, 
                          requested_start: datetime, requested_end: datetime,
                          data_type: str = 'bars') -> List[Tuple[datetime, datetime]]:
        """
        Get list of missing date ranges for the requested period.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            requested_start: Requested start date
            requested_end: Requested end date
            data_type: Type of data ('bars' or 'ticks')
            
        Returns:
            List of (start_date, end_date) tuples for missing ranges
        """
        exists, actual_start, actual_end = self.check_exists(
            symbol, timeframe, requested_start, requested_end, data_type
        )
        
        if not exists:
            # No data exists, return the full requested range
            return [(requested_start, requested_end)]
        
        missing_ranges = []
        
        # Check if we need data before the existing range
        if actual_start and requested_start < actual_start:
            missing_ranges.append((requested_start, actual_start))
        
        # Check if we need data after the existing range
        if actual_end and requested_end > actual_end:
            missing_ranges.append((actual_end, requested_end))
        
        return missing_ranges
    
    def validate_dataframe(self, df: pd.DataFrame, data_type: str) -> bool:
        """
        Validate DataFrame structure based on data type.
        
        Args:
            df: DataFrame to validate
            data_type: Type of data ('bars' or 'ticks')
            
        Returns:
            True if DataFrame is valid
            
        Raises:
            ValueError: If DataFrame structure is invalid
        """
        if df.empty:
            return True  # Empty DataFrame is valid
        
        if data_type == 'bars':
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        elif data_type == 'ticks':
            required_columns = ['timestamp', 'bid', 'ask', 'bid_volume', 'ask_volume']
        else:
            raise ValueError(f"Unknown data type: {data_type}")
        
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check timestamp column
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            raise ValueError("Timestamp column must be datetime type")
        
        return True
    
    def generate_filename(self, symbol: str, timeframe: str, data_type: str, 
                         source: str = 'unknown', extension: str = 'parquet') -> str:
        """
        Generate standardized filename for data storage.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            data_type: Type of data ('bars' or 'ticks')
            source: Data source name
            extension: File extension (default: 'parquet')
            
        Returns:
            Standardized filename
        """
        # Format: SYMBOL_TIMEFRAME_DATATYPE_SOURCE.extension
        # Example: EURUSD_1h_bars_dukascopy.parquet
        return f"{symbol}_{timeframe}_{data_type}_{source}.{extension}"
    
    def get_storage_path(self) -> str:
        """
        Get the base storage path for this backend.
        
        Returns:
            Storage path string
        """
        return self.config.get('path', './data') 