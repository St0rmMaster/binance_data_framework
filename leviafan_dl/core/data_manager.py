"""
Main data manager for LeviafanDL.

Orchestrates data sources and storage backends to provide unified access
to financial data with intelligent source selection and caching.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from .config_manager import ConfigManager
from ..sources.base_source import BaseSource
from ..sources.dukascopy_source import DukascopySource
from ..sources.binance_source import BinanceSource
from ..storage.base_storage import BaseStorage
from ..storage.local_storage import LocalStorage


class DataManager:
    """
    Main orchestrator for data sources and storage in LeviafanDL.
    
    Handles:
    - Automatic source selection (Dukascopy preferred, Binance fallback)
    - Intelligent caching and data retrieval
    - Storage backend management
    - Tick-to-bar resampling when beneficial
    """
    
    def __init__(self, storage_type: str = 'local', config_manager: Optional[ConfigManager] = None):
        """
        Initialize DataManager.
        
        Args:
            storage_type: Type of storage backend ('local', 'gdrive', 'ftp')
            config_manager: Optional ConfigManager instance. If None, creates new one.
        """
        self.config_manager = config_manager or ConfigManager()
        self.storage_type = storage_type
        
        # Initialize storage backend
        self.storage = self._initialize_storage()
        
        # Initialize data sources
        self.sources = self._initialize_sources()
        
        # Source priority order (Dukascopy first, then Binance)
        self.source_priority = ['dukascopy', 'binance']
        
        print(f"✓ DataManager initialized with {storage_type} storage")
        print(f"✓ Available sources: {list(self.sources.keys())}")
    
    def _initialize_storage(self) -> BaseStorage:
        """Initialize storage backend based on configuration."""
        storage_config = self.config_manager.get_storage_config(self.storage_type)
        
        if self.storage_type == 'local':
            return LocalStorage(storage_config)
        elif self.storage_type == 'gdrive':
            # TODO: Implement GDriveStorage
            raise NotImplementedError("Google Drive storage not yet implemented")
        elif self.storage_type == 'ftp':
            # TODO: Implement FTPStorage
            raise NotImplementedError("FTP storage not yet implemented")
        else:
            raise ValueError(f"Unknown storage type: {self.storage_type}")
    
    def _initialize_sources(self) -> Dict[str, BaseSource]:
        """Initialize all available data sources."""
        sources = {}
        
        # Initialize Dukascopy source
        try:
            dukascopy_config = self.config_manager.get_api_config('dukascopy')
            sources['dukascopy'] = DukascopySource(dukascopy_config)
            print("✓ Dukascopy source initialized")
        except Exception as e:
            print(f"⚠ Failed to initialize Dukascopy source: {e}")
        
        # Initialize Binance source
        try:
            binance_config = self.config_manager.get_api_config('binance')
            sources['binance'] = BinanceSource(binance_config)
            print("✓ Binance source initialized")
        except Exception as e:
            print(f"⚠ Failed to initialize Binance source: {e}")
        
        if not sources:
            raise RuntimeError("No data sources could be initialized")
        
        return sources
    
    def get_available_symbols(self, source: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Get available symbols from all sources or a specific source.
        
        Args:
            source: Optional source name. If None, returns symbols from all sources.
            
        Returns:
            Dictionary mapping source names to lists of available symbols
        """
        if source:
            if source not in self.sources:
                raise ValueError(f"Unknown source: {source}")
            return {source: self.sources[source].list_available_symbols()}
        
        return {
            name: src.list_available_symbols() 
            for name, src in self.sources.items()
        }
    
    def get_supported_timeframes(self, source: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Get supported timeframes from all sources or a specific source.
        
        Args:
            source: Optional source name. If None, returns timeframes from all sources.
            
        Returns:
            Dictionary mapping source names to lists of supported timeframes
        """
        if source:
            if source not in self.sources:
                raise ValueError(f"Unknown source: {source}")
            return {source: self.sources[source].get_supported_timeframes()}
        
        return {
            name: src.get_supported_timeframes() 
            for name, src in self.sources.items()
        }
    
    def _select_source(self, symbol: str, data_type: str = 'bars') -> Optional[str]:
        """
        Select the best data source for the given symbol and data type.
        
        Args:
            symbol: Trading symbol
            data_type: Type of data ('bars' or 'ticks')
            
        Returns:
            Name of selected source or None if no source supports the symbol
        """
        # For ticks, only Dukascopy is supported
        if data_type == 'ticks':
            if 'dukascopy' in self.sources and self.sources['dukascopy'].supports_symbol(symbol):
                return 'dukascopy'
            else:
                return None
        
        # For bars, try sources in priority order
        for source_name in self.source_priority:
            if source_name in self.sources:
                source = self.sources[source_name]
                if source.supports_symbol(symbol):
                    return source_name
        
        return None
    
    def fetch_data(self, symbol: str, start_date: datetime, end_date: datetime,
                   timeframe: str, data_type: str = 'bars', 
                   force_download: bool = False) -> pd.DataFrame:
        """
        Fetch financial data with intelligent source selection and caching.
        
        Args:
            symbol: Trading symbol (e.g., 'EURUSD', 'BTCUSDT')
            start_date: Start date for data
            end_date: End date for data
            timeframe: Timeframe (e.g., '1m', '1h', '1d')
            data_type: Type of data ('bars' or 'ticks')
            force_download: Force download even if data exists in storage
            
        Returns:
            DataFrame with requested data
        """
        print(f"📊 Fetching {data_type} data for {symbol} {timeframe} from {start_date} to {end_date}")
        
        # Select appropriate data source
        selected_source = self._select_source(symbol, data_type)
        if not selected_source:
            raise ValueError(f"No data source supports {symbol} for {data_type} data")
        
        source = self.sources[selected_source]
        print(f"✓ Using {selected_source} as data source")
        
        # Check if we should try to resample from ticks (Dukascopy only)
        if (data_type == 'bars' and selected_source == 'dukascopy' and 
            timeframe != 'tick' and not force_download):
            
            # Check if we have tick data that could be resampled
            tick_data = self._try_resample_from_ticks(symbol, start_date, end_date, timeframe)
            if tick_data is not None and not tick_data.empty:
                print(f"✓ Resampled {len(tick_data)} bars from existing tick data")
                return tick_data
        
        # Check existing data in storage
        if not force_download:
            existing_data = self._get_existing_data(symbol, timeframe, start_date, end_date, data_type)
            if existing_data is not None:
                return existing_data
        
        # Determine what data ranges we need to download
        missing_ranges = self.storage.get_missing_ranges(
            symbol, timeframe, start_date, end_date, data_type
        )
        
        if not missing_ranges and not force_download:
            # All data exists, load and return it
            return self.storage.load(symbol, timeframe, start_date, end_date, data_type)
        
        # Download missing data
        all_new_data = []
        for range_start, range_end in missing_ranges:
            print(f"📥 Downloading {data_type} from {selected_source}: {range_start} to {range_end}")
            
            try:
                if data_type == 'bars':
                    new_data = source.get_data(symbol, range_start, range_end, timeframe)
                else:  # ticks
                    new_data = source.get_ticks(symbol, range_start, range_end)
                
                if not new_data.empty:
                    # Save to storage
                    self.storage.save(new_data, symbol, timeframe, data_type, selected_source)
                    all_new_data.append(new_data)
                    print(f"✓ Downloaded and saved {len(new_data)} {data_type} records")
                else:
                    print(f"⚠ No data available for {symbol} in range {range_start} to {range_end}")
                    
            except Exception as e:
                print(f"✗ Failed to download data from {selected_source}: {e}")
                continue
        
        # Load complete dataset from storage
        final_data = self.storage.load(symbol, timeframe, start_date, end_date, data_type)
        
        if final_data.empty:
            print(f"⚠ No data available for {symbol} {timeframe} in the requested period")
        else:
            print(f"✓ Returning {len(final_data)} {data_type} records for {symbol} {timeframe}")
        
        return final_data
    
    def _get_existing_data(self, symbol: str, timeframe: str, start_date: datetime,
                          end_date: datetime, data_type: str) -> Optional[pd.DataFrame]:
        """
        Check if we have complete data in storage for the requested period.
        
        Returns:
            DataFrame if complete data exists, None otherwise
        """
        exists, actual_start, actual_end = self.storage.check_exists(
            symbol, timeframe, start_date, end_date, data_type
        )
        
        if exists and actual_start and actual_end:
            # Check if existing data covers the requested period
            if actual_start <= start_date and actual_end >= end_date:
                print(f"✓ Found complete {data_type} data in storage")
                return self.storage.load(symbol, timeframe, start_date, end_date, data_type)
        
        return None
    
    def _try_resample_from_ticks(self, symbol: str, start_date: datetime, 
                                end_date: datetime, timeframe: str) -> Optional[pd.DataFrame]:
        """
        Try to resample bar data from existing tick data.
        
        Returns:
            Resampled DataFrame or None if not possible
        """
        try:
            # Check if we have tick data for this period
            exists, tick_start, tick_end = self.storage.check_exists(
                symbol, 'tick', start_date, end_date, 'ticks'
            )
            
            if exists and tick_start and tick_end:
                if tick_start <= start_date and tick_end >= end_date:
                    print(f"✓ Found tick data, attempting to resample to {timeframe}")
                    
                    # Load tick data
                    tick_data = self.storage.load(symbol, 'tick', start_date, end_date, 'ticks')
                    
                    if not tick_data.empty and 'dukascopy' in self.sources:
                        # Use Dukascopy source's resampling method
                        dukascopy_source = self.sources['dukascopy']
                        if hasattr(dukascopy_source, 'resample_ticks_to_bars'):
                            resampled = dukascopy_source.resample_ticks_to_bars(tick_data, timeframe)
                            
                            if not resampled.empty:
                                # Save resampled data
                                self.storage.save(resampled, symbol, timeframe, 'bars', 'dukascopy_resampled')
                                return resampled
            
            return None
            
        except Exception as e:
            print(f"⚠ Failed to resample from ticks: {e}")
            return None
    
    def get_stored_info(self) -> Dict[str, Any]:
        """Get information about data stored in the current storage backend."""
        return self.storage.get_stored_info()
    
    def delete_data(self, symbol: str, timeframe: str, data_type: str = 'bars') -> bool:
        """
        Delete stored data for given parameters.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            data_type: Type of data ('bars' or 'ticks')
            
        Returns:
            True if deletion successful
        """
        return self.storage.delete_data(symbol, timeframe, data_type)
    
    def validate_request(self, symbol: str, timeframe: str, data_type: str = 'bars') -> bool:
        """
        Validate if a data request can be fulfilled.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe
            data_type: Type of data ('bars' or 'ticks')
            
        Returns:
            True if request is valid and can be fulfilled
        """
        selected_source = self._select_source(symbol, data_type)
        if not selected_source:
            return False
        
        source = self.sources[selected_source]
        return source.supports_timeframe(timeframe)
    
    def close(self):
        """Close connections and cleanup resources."""
        if hasattr(self.storage, 'close'):
            self.storage.close()
        print("✓ DataManager closed")