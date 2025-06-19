"""
Base abstract class for data sources in LeviafanDL.

Defines the common interface that all data sources must implement.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from datetime import datetime
import pandas as pd


class BaseSource(ABC):
    """
    Abstract base class for financial data sources.
    
    All data source implementations (Dukascopy, Binance, etc.) must inherit
    from this class and implement the required methods.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the data source with configuration.
        
        Args:
            config: Configuration dictionary from ConfigManager
        """
        self.config = config
        self.name = self.__class__.__name__.replace('Source', '').lower()
    
    @abstractmethod
    def get_data(self, symbol: str, start_date: datetime, end_date: datetime, 
                 timeframe: str) -> pd.DataFrame:
        """
        Get OHLCV bar data for the specified parameters.
        
        Args:
            symbol: Trading symbol (e.g., 'EURUSD', 'BTCUSDT')
            start_date: Start date for data
            end_date: End date for data
            timeframe: Timeframe (e.g., '1m', '1h', '1d')
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
            
        Raises:
            NotImplementedError: If the source doesn't support bar data
            ValueError: If parameters are invalid
        """
        pass
    
    @abstractmethod
    def get_ticks(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Get tick data for the specified parameters.
        
        Args:
            symbol: Trading symbol (e.g., 'EURUSD', 'BTCUSDT')
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            DataFrame with columns: timestamp, bid, ask, bid_volume, ask_volume
            
        Raises:
            NotImplementedError: If the source doesn't support tick data
            ValueError: If parameters are invalid
        """
        pass
    
    @abstractmethod
    def list_available_symbols(self) -> List[str]:
        """
        Get list of available symbols from this data source.
        
        Returns:
            List of available trading symbols
        """
        pass
    
    @abstractmethod
    def supports_symbol(self, symbol: str) -> bool:
        """
        Check if this data source supports the given symbol.
        
        Args:
            symbol: Trading symbol to check
            
        Returns:
            True if symbol is supported, False otherwise
        """
        pass
    
    @abstractmethod
    def supports_timeframe(self, timeframe: str) -> bool:
        """
        Check if this data source supports the given timeframe.
        
        Args:
            timeframe: Timeframe to check (e.g., '1m', '1h', '1d')
            
        Returns:
            True if timeframe is supported, False otherwise
        """
        pass
    
    def get_supported_timeframes(self) -> List[str]:
        """
        Get list of supported timeframes for this data source.
        
        Returns:
            List of supported timeframes
        """
        # Default implementation - subclasses should override if needed
        return ['1m', '5m', '15m', '30m', '1h', '4h', '1d']
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol format for this data source.
        
        Args:
            symbol: Input symbol
            
        Returns:
            Normalized symbol format for this source
        """
        # Default implementation - subclasses should override if needed
        return symbol.upper()
    
    def validate_parameters(self, symbol: str, start_date: datetime, 
                          end_date: datetime, timeframe: Optional[str] = None) -> bool:
        """
        Validate input parameters for data requests.
        
        Args:
            symbol: Trading symbol
            start_date: Start date
            end_date: End date
            timeframe: Timeframe (optional for tick data)
            
        Returns:
            True if parameters are valid
            
        Raises:
            ValueError: If parameters are invalid
        """
        if not symbol:
            raise ValueError("Symbol cannot be empty")
        
        if start_date >= end_date:
            raise ValueError("Start date must be before end date")
        
        if not self.supports_symbol(symbol):
            raise ValueError(f"Symbol '{symbol}' not supported by {self.name}")
        
        if timeframe and not self.supports_timeframe(timeframe):
            raise ValueError(f"Timeframe '{timeframe}' not supported by {self.name}")
        
        return True 