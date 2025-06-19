"""
Binance data source implementation for LeviafanDL.

Provides access to cryptocurrency data from Binance US API.
"""

import pandas as pd
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from .base_source import BaseSource


class BinanceSource(BaseSource):
    """
    Data source implementation for Binance cryptocurrency data.
    
    Supports cryptocurrency trading pairs available on Binance US.
    """
    
    # Supported timeframes mapping (standard format -> Binance format)
    TIMEFRAME_MAP = {
        '1m': '1m',
        '3m': '3m', 
        '5m': '5m',
        '15m': '15m',
        '30m': '30m',
        '1h': '1h',
        '2h': '2h',
        '4h': '4h',
        '6h': '6h',
        '8h': '8h',
        '12h': '12h',
        '1d': '1d',
        '3d': '3d',
        '1w': '1w',
        '1M': '1M'
    }
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Binance data source."""
        super().__init__(config)
        self.api_key = config.get('api_key')
        self.api_secret = config.get('api_secret')
        self.testnet = config.get('testnet', False)
        self.client = None
        self._available_symbols = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the Binance client."""
        try:
            from binance.client import Client
            from binance.exceptions import BinanceAPIException
            
            # Store exceptions for later use
            self.BinanceAPIException = BinanceAPIException
            
            # Initialize client
            tld = 'us' if not self.testnet else 'com'  # Use 'us' for production, 'com' for testnet
            self.client = Client(
                api_key=self.api_key,
                api_secret=self.api_secret,
                tld=tld
            )
            
            # Test connection if API keys are provided
            if self.api_key and self.api_secret:
                try:
                    self.client.ping()
                except Exception as e:
                    print(f"⚠ Binance API connection test failed: {e}")
            
        except ImportError:
            raise ImportError(
                "python-binance is required for BinanceSource. "
                "Install it with: pip install python-binance"
            )
    
    def get_data(self, symbol: str, start_date: datetime, end_date: datetime, 
                 timeframe: str) -> pd.DataFrame:
        """
        Get OHLCV bar data from Binance.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            start_date: Start date for data
            end_date: End date for data
            timeframe: Timeframe (e.g., '1m', '1h', '1d')
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        self.validate_parameters(symbol, start_date, end_date, timeframe)
        
        normalized_symbol = self.normalize_symbol(symbol)
        binance_timeframe = self.TIMEFRAME_MAP.get(timeframe)
        
        if not binance_timeframe:
            raise ValueError(f"Timeframe '{timeframe}' not supported")
        
        try:
            if self.client is None:
                raise RuntimeError("Binance client not initialized")
            
            # Convert datetime to timestamps
            start_ts = int(start_date.timestamp() * 1000)
            end_ts = int(end_date.timestamp() * 1000)
            
            # Initialize list for all klines
            all_klines = []
            max_limit = 1000
            current_start = start_ts
            
            # Download data with pagination
            while current_start < end_ts:
                try:
                    klines = self.client.get_historical_klines(
                        symbol=normalized_symbol,
                        interval=binance_timeframe,
                        start_str=current_start,
                        end_str=end_ts,
                        limit=max_limit
                    )
                    
                    if not klines:
                        break
                    
                    all_klines.extend(klines)
                    current_start = klines[-1][0] + 1
                    
                    # Rate limiting
                    time.sleep(0.1)
                    
                except self.BinanceAPIException as e:
                    if 'Too much request weight used' in str(e):
                        print("⚠ Rate limit exceeded, waiting 60 seconds...")
                        time.sleep(60)
                        continue
                    else:
                        raise RuntimeError(f"Binance API error: {str(e)}")
                        
            if not all_klines:
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Convert to DataFrame
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                      'close_time', 'quote_asset_volume', 'number_of_trades', 
                      'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
            
            df = pd.DataFrame(all_klines, columns=columns)
            
            # Convert data types
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)
            
            # Convert timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Return only required columns
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            
        except Exception as e:
            raise RuntimeError(f"Failed to download data from Binance: {str(e)}")
    
    def get_ticks(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Get tick data from Binance.
        
        Note: Binance public API doesn't provide tick data directly.
        This method raises NotImplementedError.
        
        Args:
            symbol: Trading symbol
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            DataFrame with tick data (not implemented)
            
        Raises:
            NotImplementedError: Binance doesn't provide tick data via public API
        """
        raise NotImplementedError(
            "Binance public API doesn't provide tick data. "
            "Use get_data() method to get OHLCV bar data instead."
        )
    
    def list_available_symbols(self) -> List[str]:
        """Get list of available symbols from Binance."""
        if self._available_symbols is None:
            self._available_symbols = self._fetch_available_symbols()
        return self._available_symbols
    
    def _fetch_available_symbols(self) -> List[str]:
        """Fetch available symbols from Binance API."""
        try:
            if self.client is None:
                raise RuntimeError("Binance client not initialized")
            
            exchange_info = self.client.get_exchange_info()
            
            if not exchange_info or 'symbols' not in exchange_info:
                return []
            
            # Get all active trading symbols
            symbols = [
                symbol['symbol'] for symbol in exchange_info['symbols']
                if symbol['status'] == 'TRADING'
            ]
            
            return sorted(symbols)
            
        except Exception as e:
            print(f"⚠ Failed to fetch available symbols from Binance: {e}")
            # Return common USDT pairs as fallback
            return [
                'BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'DOTUSDT', 'LINKUSDT',
                'LTCUSDT', 'XRPUSDT', 'BCHUSD', 'XLMUSDT', 'UNIUSDT'
            ]
    
    def supports_symbol(self, symbol: str) -> bool:
        """Check if symbol is supported by Binance."""
        normalized_symbol = self.normalize_symbol(symbol)
        return normalized_symbol in self.list_available_symbols()
    
    def supports_timeframe(self, timeframe: str) -> bool:
        """Check if timeframe is supported."""
        return timeframe in self.TIMEFRAME_MAP
    
    def get_supported_timeframes(self) -> List[str]:
        """Get list of supported timeframes."""
        return list(self.TIMEFRAME_MAP.keys())
    
    def normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol format for Binance.
        
        Args:
            symbol: Input symbol
            
        Returns:
            Normalized symbol in Binance format (uppercase, no separators)
        """
        # Remove common separators and convert to uppercase
        normalized = symbol.replace('/', '').replace('-', '').replace('_', '').upper()
        
        # Handle common symbol mappings
        symbol_mappings = {
            'BCHUSD': 'BCHUSD',  # Bitcoin Cash USD (Binance US specific)
            'BCHUSDT': 'BCHUSD'  # Map USDT to USD for BCH on Binance US
        }
        
        return symbol_mappings.get(normalized, normalized) 