"""
Local storage implementation for LeviafanDL using DuckDB.

Provides high-performance local storage for financial data using DuckDB database.
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from .base_storage import BaseStorage


class LocalStorage(BaseStorage):
    """
    Local storage implementation using DuckDB.
    
    Stores financial data in DuckDB database files for high-performance
    local access and analysis.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize local storage with DuckDB."""
        super().__init__(config)
        self.storage_path = Path(config.get('path', './data'))
        self.db_name = config.get('db_name', 'leviafan_data.duckdb')
        self.db_path = self.storage_path / self.db_name
        self.conn = None
        self.duckdb = None
        
        # Create storage directory if it doesn't exist
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize DuckDB connection and create tables."""
        try:
            import duckdb
            self.duckdb = duckdb
            
            # Connect to DuckDB
            self.conn = duckdb.connect(str(self.db_path))
            
            # Create tables for bars and ticks data
            self._create_tables()
            
            print(f"✓ Local storage initialized: {self.db_path}")
            
        except ImportError:
            raise ImportError(
                "duckdb is required for LocalStorage. "
                "Install it with: pip install duckdb"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize local storage: {e}")
    
    def _create_tables(self):
        """Create necessary tables in DuckDB."""
        # Table for OHLCV bar data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bars_data (
                timestamp TIMESTAMP,
                symbol VARCHAR,
                timeframe VARCHAR,
                source VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY (timestamp, symbol, timeframe, source)
            )
        """)
        
        # Table for tick data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ticks_data (
                timestamp TIMESTAMP,
                symbol VARCHAR,
                source VARCHAR,
                bid DOUBLE,
                ask DOUBLE,
                bid_volume DOUBLE,
                ask_volume DOUBLE,
                PRIMARY KEY (timestamp, symbol, source)
            )
        """)
        
        # Metadata table to track data ranges
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS data_metadata (
                symbol VARCHAR,
                timeframe VARCHAR,
                data_type VARCHAR,
                source VARCHAR,
                start_timestamp TIMESTAMP,
                end_timestamp TIMESTAMP,
                record_count INTEGER,
                last_updated TIMESTAMP,
                PRIMARY KEY (symbol, timeframe, data_type, source)
            )
        """)
        
        # Create indexes for better performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_bars_symbol_timeframe ON bars_data (symbol, timeframe)",
            "CREATE INDEX IF NOT EXISTS idx_bars_timestamp ON bars_data (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_ticks_symbol ON ticks_data (symbol)",
            "CREATE INDEX IF NOT EXISTS idx_ticks_timestamp ON ticks_data (timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_metadata_symbol ON data_metadata (symbol)"
        ]
        
        for index_sql in indexes:
            self.conn.execute(index_sql)
    
    def save(self, df: pd.DataFrame, symbol: str, timeframe: str, 
             data_type: str = 'bars', source: str = 'unknown') -> bool:
        """Save DataFrame to local DuckDB storage."""
        try:
            if df.empty:
                print(f"⚠ Empty DataFrame provided for {symbol} {timeframe}")
                return True
            
            # Validate DataFrame
            self.validate_dataframe(df, data_type)
            
            # Prepare data for insertion
            df_copy = df.copy()
            df_copy['symbol'] = symbol
            df_copy['timeframe'] = timeframe if data_type == 'bars' else 'tick'
            df_copy['source'] = source
            
            # Choose table based on data type
            table_name = 'bars_data' if data_type == 'bars' else 'ticks_data'
            
            # Insert data (replace existing data for the same parameters)
            if data_type == 'bars':
                # Remove existing data for this symbol/timeframe/source
                self.conn.execute("""
                    DELETE FROM bars_data 
                    WHERE symbol = ? AND timeframe = ? AND source = ?
                """, [symbol, timeframe, source])
                
                # Insert new data
                self.conn.execute(f"""
                    INSERT INTO {table_name} 
                    SELECT * FROM df_copy
                """)
            else:  # ticks
                # Remove existing data for this symbol/source in the time range
                min_ts = df_copy['timestamp'].min()
                max_ts = df_copy['timestamp'].max()
                
                self.conn.execute("""
                    DELETE FROM ticks_data 
                    WHERE symbol = ? AND source = ? 
                    AND timestamp >= ? AND timestamp <= ?
                """, [symbol, source, min_ts, max_ts])
                
                # Insert new data
                self.conn.execute(f"""
                    INSERT INTO {table_name} 
                    SELECT * FROM df_copy
                """)
            
            # Update metadata
            self._update_metadata(df_copy, symbol, timeframe, data_type, source)
            
            print(f"✓ Saved {len(df)} {data_type} records for {symbol} {timeframe} from {source}")
            return True
            
        except Exception as e:
            print(f"✗ Failed to save data for {symbol} {timeframe}: {e}")
            return False
    
    def _update_metadata(self, df: pd.DataFrame, symbol: str, timeframe: str, 
                        data_type: str, source: str):
        """Update metadata table with information about stored data."""
        min_ts = df['timestamp'].min()
        max_ts = df['timestamp'].max()
        record_count = len(df)
        current_time = datetime.now()
        
        # Upsert metadata
        self.conn.execute("""
            INSERT OR REPLACE INTO data_metadata 
            (symbol, timeframe, data_type, source, start_timestamp, end_timestamp, 
             record_count, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [symbol, timeframe, data_type, source, min_ts, max_ts, 
              record_count, current_time])
    
    def load(self, symbol: str, timeframe: str, start_date: datetime, 
             end_date: datetime, data_type: str = 'bars') -> pd.DataFrame:
        """Load data from local DuckDB storage."""
        try:
            table_name = 'bars_data' if data_type == 'bars' else 'ticks_data'
            
            if data_type == 'bars':
                query = f"""
                    SELECT timestamp, open, high, low, close, volume
                    FROM {table_name}
                    WHERE symbol = ? AND timeframe = ?
                    AND timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                """
                params = [symbol, timeframe, start_date, end_date]
            else:  # ticks
                query = f"""
                    SELECT timestamp, bid, ask, bid_volume, ask_volume
                    FROM {table_name}
                    WHERE symbol = ?
                    AND timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                """
                params = [symbol, start_date, end_date]
            
            df = self.conn.execute(query, params).df()
            
            if df.empty:
                # Return empty DataFrame with correct columns
                if data_type == 'bars':
                    columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                else:
                    columns = ['timestamp', 'bid', 'ask', 'bid_volume', 'ask_volume']
                return pd.DataFrame(columns=columns)
            
            return df
            
        except Exception as e:
            print(f"✗ Failed to load data for {symbol} {timeframe}: {e}")
            if data_type == 'bars':
                columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            else:
                columns = ['timestamp', 'bid', 'ask', 'bid_volume', 'ask_volume']
            return pd.DataFrame(columns=columns)
    
    def check_exists(self, symbol: str, timeframe: str, start_date: datetime, 
                    end_date: datetime, data_type: str = 'bars') -> Tuple[bool, Optional[datetime], Optional[datetime]]:
        """Check if data exists in storage for given parameters."""
        try:
            # Query metadata table
            query = """
                SELECT start_timestamp, end_timestamp
                FROM data_metadata
                WHERE symbol = ? AND timeframe = ? AND data_type = ?
            """
            
            result = self.conn.execute(query, [symbol, timeframe, data_type]).fetchone()
            
            if result is None:
                return False, None, None
            
            actual_start, actual_end = result
            
            # Convert to datetime if needed
            if isinstance(actual_start, str):
                actual_start = pd.to_datetime(actual_start).to_pydatetime()
            if isinstance(actual_end, str):
                actual_end = pd.to_datetime(actual_end).to_pydatetime()
            
            return True, actual_start, actual_end
            
        except Exception as e:
            print(f"✗ Failed to check data existence for {symbol} {timeframe}: {e}")
            return False, None, None
    
    def get_stored_info(self) -> Dict[str, Any]:
        """Get information about stored data."""
        try:
            # Get summary from metadata table
            query = """
                SELECT symbol, timeframe, data_type, source, 
                       start_timestamp, end_timestamp, record_count, last_updated
                FROM data_metadata
                ORDER BY symbol, timeframe, data_type
            """
            
            df = self.conn.execute(query).df()
            
            if df.empty:
                return {
                    'summary': 'No data stored',
                    'symbols': [],
                    'timeframes': [],
                    'data_types': [],
                    'sources': []
                }
            
            return {
                'summary': f"Stored data for {len(df)} symbol/timeframe/type combinations",
                'symbols': sorted(df['symbol'].unique().tolist()),
                'timeframes': sorted(df['timeframe'].unique().tolist()),
                'data_types': sorted(df['data_type'].unique().tolist()),
                'sources': sorted(df['source'].unique().tolist()),
                'details': df.to_dict('records')
            }
            
        except Exception as e:
            print(f"✗ Failed to get stored info: {e}")
            return {'error': str(e)}
    
    def delete_data(self, symbol: str, timeframe: str, 
                   data_type: str = 'bars') -> bool:
        """Delete stored data for given parameters."""
        try:
            table_name = 'bars_data' if data_type == 'bars' else 'ticks_data'
            
            if data_type == 'bars':
                # Delete from main table
                self.conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE symbol = ? AND timeframe = ?
                """, [symbol, timeframe])
            else:  # ticks
                # Delete from main table
                self.conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE symbol = ?
                """, [symbol])
            
            # Delete from metadata
            self.conn.execute("""
                DELETE FROM data_metadata
                WHERE symbol = ? AND timeframe = ? AND data_type = ?
            """, [symbol, timeframe, data_type])
            
            print(f"✓ Deleted {data_type} data for {symbol} {timeframe}")
            return True
            
        except Exception as e:
            print(f"✗ Failed to delete data for {symbol} {timeframe}: {e}")
            return False
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __del__(self):
        """Cleanup on object destruction."""
        self.close() 