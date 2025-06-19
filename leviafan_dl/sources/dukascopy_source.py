"""
Dukascopy data source implementation for LeviafanDL.

Provides access to Forex, CFDs, commodities, and other financial instruments
from Dukascopy's historical data feed.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from .base_source import BaseSource


class DukascopySource(BaseSource):
    """
    Data source implementation for Dukascopy historical data.
    
    Supports:
    - Forex pairs (EURUSD, GBPUSD, etc.)
    - Precious metals (XAUUSD, XAGUSD)
    - Commodities (BRENT.CMD/USD, WTI.CMD/USD, etc.)
    - Stock indices (USA500.IDX/USD, DEU.IDX/EUR, etc.)
    - Cryptocurrencies (BTCUSD, ETHUSD, etc.)
    - Individual stocks as CFDs (AAPL.US, BMW.DE, etc.)
    """
    
    # Dukascopy symbol categories and their available symbols
    FOREX_PAIRS = [
        'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'USDCAD', 'NZDUSD',
        'EURGBP', 'EURJPY', 'EURCHF', 'EURAUD', 'EURCAD', 'EURNZD',
        'GBPJPY', 'GBPCHF', 'GBPAUD', 'GBPCAD', 'GBPNZD',
        'AUDJPY', 'AUDCHF', 'AUDCAD', 'AUDNZD',
        'CADJPY', 'CADCHF', 'NZDJPY', 'NZDCHF', 'NZDCAD',
        'CHFJPY', 'USDCNH', 'USDSEK', 'USDNOK', 'USDDKK', 'USDPLN'
    ]
    
    METALS = [
        'XAUUSD', 'XAGUSD', 'XPTUSD', 'XPDUSD'
    ]
    
    COMMODITIES = [
        'BRENT.CMD/USD', 'WTI.CMD/USD', 'COFFEE.CMD/USD', 'CORN.CMD/USD',
        'SUGAR.CMD/USD', 'WHEAT.CMD/USD', 'NATGAS.CMD/USD'
    ]
    
    INDICES = [
        'USA500.IDX/USD', 'USA30.IDX/USD', 'USA100.IDX/USD',
        'DEU.IDX/EUR', 'GBR.IDX/GBP', 'FRA.IDX/EUR', 'JPN.IDX/JPY'
    ]
    
    CRYPTO = [
        'BTCUSD', 'ETHUSD', 'LTCUSD', 'XRPUSD', 'BCHUSD', 'ADAUSD',
        'DOTUSD', 'LINKUSD', 'XLMUSD', 'EOSUSD', 'XMRUSD', 'DASHUSD',
        'ZECUSD', 'ETCUSD', 'TRXUSD', 'VETUSD', 'QTMUSD', 'OMGUSD'
    ]
    
    # Popular stock CFDs (partial list)
    STOCKS = [
        'AAPL.US', 'MSFT.US', 'GOOGL.US', 'AMZN.US', 'TSLA.US', 'META.US',
        'NVDA.US', 'JPM.US', 'JNJ.US', 'PG.US', 'V.US', 'UNH.US',
        'BMW.DE', 'SAP.DE', 'SIE.DE', 'ALV.DE', 'BAS.DE', 'BAYER.DE'
    ]
    
    # Supported timeframes mapping (Dukascopy format -> standard format)
    TIMEFRAME_MAP = {
        'tick': 'tick',
        '1m': 'm1',
        '5m': 'm5', 
        '15m': 'm15',
        '30m': 'm30',
        '1h': 'h1',
        '4h': 'h4',
        '1d': 'd1'
    }
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize Dukascopy data source."""
        super().__init__(config)
        self.timeout = config.get('timeout', 30)
        self.retries = config.get('retries', 3)
        self._downloader = None
        self._initialize_downloader()
    
    def _initialize_downloader(self):
        """Initialize the Dukascopy downloader."""
        try:
            from dukascopy import Downloader
            self._downloader = Downloader(
                threads=4,  # Conservative thread count to avoid HTTP 403
                buffer_size=256
            )
        except ImportError:
            raise ImportError(
                "dukascopy-python is required for DukascopySource. "
                "Install it with: pip install dukascopy-python"
            )
    
    def get_data(self, symbol: str, start_date: datetime, end_date: datetime, 
                 timeframe: str) -> pd.DataFrame:
        """
        Get OHLCV bar data from Dukascopy.
        
        Args:
            symbol: Trading symbol (e.g., 'EURUSD', 'BTCUSD')
            start_date: Start date for data
            end_date: End date for data  
            timeframe: Timeframe (e.g., '1m', '1h', '1d')
            
        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        self.validate_parameters(symbol, start_date, end_date, timeframe)
        
        normalized_symbol = self.normalize_symbol(symbol)
        dukascopy_timeframe = self.TIMEFRAME_MAP.get(timeframe)
        
        if not dukascopy_timeframe:
            raise ValueError(f"Timeframe '{timeframe}' not supported")
        
        try:
            if self._downloader is None:
                raise RuntimeError("Dukascopy downloader not initialized")
                
            # Download data using dukascopy-python
            df = self._downloader.download(
                instrument=normalized_symbol.lower(),
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                timeframe=dukascopy_timeframe
            )
            
            if df.empty:
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Standardize column names
            df = df.reset_index()
            df.rename(columns={
                'time': 'timestamp',
                'o': 'open',
                'h': 'high', 
                'l': 'low',
                'c': 'close',
                'v': 'volume'
            }, inplace=True)
            
            # Ensure all required columns exist
            required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            for col in required_cols:
                if col not in df.columns:
                    if col == 'volume':
                        df[col] = 0.0  # Some instruments don't have volume data
                    else:
                        raise ValueError(f"Missing required column: {col}")
            
            return df[required_cols]
            
        except Exception as e:
            raise RuntimeError(f"Failed to download data from Dukascopy: {str(e)}")
    
    def get_ticks(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Get tick data from Dukascopy.
        
        Args:
            symbol: Trading symbol
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            DataFrame with columns: timestamp, bid, ask, bid_volume, ask_volume
        """
        self.validate_parameters(symbol, start_date, end_date)
        
        normalized_symbol = self.normalize_symbol(symbol)
        
        try:
            if self._downloader is None:
                raise RuntimeError("Dukascopy downloader not initialized")
                
            # Download tick data
            df = self._downloader.download(
                instrument=normalized_symbol.lower(),
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                timeframe='tick'
            )
            
            if df.empty:
                return pd.DataFrame(columns=['timestamp', 'bid', 'ask', 'bid_volume', 'ask_volume'])
            
            # Standardize column names for tick data
            df = df.reset_index()
            df.rename(columns={
                'time': 'timestamp',
                'bidprice': 'bid',
                'askprice': 'ask',
                'bidvolume': 'bid_volume',
                'askvolume': 'ask_volume'
            }, inplace=True)
            
            # Ensure all required columns exist
            required_cols = ['timestamp', 'bid', 'ask', 'bid_volume', 'ask_volume']
            for col in required_cols:
                if col not in df.columns:
                    if col in ['bid_volume', 'ask_volume']:
                        df[col] = 1.0  # Default volume for ticks
                    else:
                        raise ValueError(f"Missing required column: {col}")
            
            return df[required_cols]
            
        except Exception as e:
            raise RuntimeError(f"Failed to download tick data from Dukascopy: {str(e)}")
    
    def resample_ticks_to_bars(self, tick_df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        Resample tick data to OHLCV bars.
        
        Args:
            tick_df: DataFrame with tick data
            timeframe: Target timeframe (e.g., '1m', '1h', '1d')
            
        Returns:
            DataFrame with OHLCV bar data
        """
        if tick_df.empty:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Convert timestamp to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(tick_df['timestamp']):
            tick_df['timestamp'] = pd.to_datetime(tick_df['timestamp'])
        
        # Use mid price for OHLC calculation
        tick_df['mid_price'] = (tick_df['bid'] + tick_df['ask']) / 2
        tick_df['volume'] = tick_df['bid_volume'] + tick_df['ask_volume']
        
        # Set timestamp as index for resampling
        tick_df.set_index('timestamp', inplace=True)
        
        # Map timeframe to pandas frequency
        freq_map = {
            '1m': '1T',
            '5m': '5T',
            '15m': '15T', 
            '30m': '30T',
            '1h': '1H',
            '4h': '4H',
            '1d': '1D'
        }
        
        freq = freq_map.get(timeframe)
        if not freq:
            raise ValueError(f"Timeframe '{timeframe}' not supported for resampling")
        
        # Resample to OHLCV
        resampled = tick_df['mid_price'].resample(freq, label='left', closed='left').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min', 
            'close': 'last'
        })
        
        # Add volume
        resampled['volume'] = tick_df['volume'].resample(freq, label='left', closed='left').sum()
        
        # Reset index and clean up
        resampled.reset_index(inplace=True)
        resampled.dropna(inplace=True)
        
        return resampled
    
    def list_available_symbols(self) -> List[str]:
        """Get list of all supported symbols."""
        return (self.FOREX_PAIRS + self.METALS + self.COMMODITIES + 
                self.INDICES + self.CRYPTO + self.STOCKS)
    
    def supports_symbol(self, symbol: str) -> bool:
        """Check if symbol is supported by Dukascopy."""
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
        Normalize symbol format for Dukascopy.
        
        Args:
            symbol: Input symbol
            
        Returns:
            Normalized symbol in Dukascopy format
        """
        symbol = symbol.upper()
        
        # Handle different symbol formats
        if '/' in symbol:
            # Already in correct format (e.g., 'BRENT.CMD/USD')
            return symbol
        elif '.' in symbol:
            # Stock or index format (e.g., 'AAPL.US')
            return symbol
        else:
            # Forex pair format (e.g., 'EURUSD')
            return symbol 