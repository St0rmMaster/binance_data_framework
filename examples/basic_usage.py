#!/usr/bin/env python3
"""
Basic usage example for LeviafanDL framework.

This example demonstrates:
1. Initializing the DataManager
2. Fetching Forex data from Dukascopy
3. Fetching cryptocurrency data from Binance
4. Using local storage for caching
"""

import sys
import os
from datetime import datetime, timedelta

# Add the parent directory to Python path to import leviafan_dl
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from leviafan_dl import DataManager, ConfigManager


def main():
    """Demonstrate basic usage of LeviafanDL framework."""
    
    print("🚀 LeviafanDL Framework - Basic Usage Example")
    print("=" * 50)
    
    try:
        # Initialize ConfigManager (will auto-detect environment)
        config_manager = ConfigManager()
        
        # Initialize DataManager with local storage
        data_manager = DataManager(storage_type='local', config_manager=config_manager)
        
        print("\n📋 Available symbols and timeframes:")
        print("-" * 30)
        
        # Show available symbols from each source
        available_symbols = data_manager.get_available_symbols()
        for source, symbols in available_symbols.items():
            print(f"{source.upper()}: {len(symbols)} symbols available")
            print(f"  Examples: {symbols[:5]}...")  # Show first 5 symbols
        
        # Show supported timeframes
        timeframes = data_manager.get_supported_timeframes()
        for source, tf_list in timeframes.items():
            print(f"{source.upper()} timeframes: {tf_list}")
        
        print("\n💾 Current storage info:")
        print("-" * 25)
        stored_info = data_manager.get_stored_info()
        print(stored_info.get('summary', 'No stored data'))
        
        # Define date range for examples (last 7 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print(f"\n📊 Fetching data examples ({start_date.date()} to {end_date.date()}):")
        print("-" * 60)
        
        # Example 1: Fetch Forex data (EURUSD) from Dukascopy
        print("\n1. Fetching EURUSD 1-hour bars from Dukascopy...")
        try:
            eurusd_data = data_manager.fetch_data(
                symbol='EURUSD',
                start_date=start_date,
                end_date=end_date,
                timeframe='1h',
                data_type='bars'
            )
            
            if not eurusd_data.empty:
                print(f"   ✓ Retrieved {len(eurusd_data)} EURUSD bars")
                print(f"   📈 Price range: {eurusd_data['close'].min():.5f} - {eurusd_data['close'].max():.5f}")
                print(f"   📅 Time range: {eurusd_data['timestamp'].min()} to {eurusd_data['timestamp'].max()}")
            else:
                print("   ⚠ No EURUSD data available for this period")
                
        except Exception as e:
            print(f"   ✗ Failed to fetch EURUSD data: {e}")
        
        # Example 2: Fetch cryptocurrency data (BTCUSDT) from Binance
        print("\n2. Fetching BTCUSDT 1-hour bars from Binance...")
        try:
            btc_data = data_manager.fetch_data(
                symbol='BTCUSDT',
                start_date=start_date,
                end_date=end_date,
                timeframe='1h',
                data_type='bars'
            )
            
            if not btc_data.empty:
                print(f"   ✓ Retrieved {len(btc_data)} BTCUSDT bars")
                print(f"   📈 Price range: ${btc_data['close'].min():.2f} - ${btc_data['close'].max():.2f}")
                print(f"   📅 Time range: {btc_data['timestamp'].min()} to {btc_data['timestamp'].max()}")
            else:
                print("   ⚠ No BTCUSDT data available for this period")
                
        except Exception as e:
            print(f"   ✗ Failed to fetch BTCUSDT data: {e}")
        
        # Example 3: Try to fetch tick data (Dukascopy only)
        print("\n3. Fetching EURUSD tick data from Dukascopy...")
        try:
            # Use a shorter time range for tick data (last 2 hours)
            tick_end = datetime.now()
            tick_start = tick_end - timedelta(hours=2)
            
            tick_data = data_manager.fetch_data(
                symbol='EURUSD',
                start_date=tick_start,
                end_date=tick_end,
                timeframe='tick',
                data_type='ticks'
            )
            
            if not tick_data.empty:
                print(f"   ✓ Retrieved {len(tick_data)} EURUSD ticks")
                print(f"   📈 Bid range: {tick_data['bid'].min():.5f} - {tick_data['bid'].max():.5f}")
                print(f"   📈 Ask range: {tick_data['ask'].min():.5f} - {tick_data['ask'].max():.5f}")
            else:
                print("   ⚠ No tick data available for this period")
                
        except Exception as e:
            print(f"   ✗ Failed to fetch tick data: {e}")
        
        # Example 4: Show automatic source selection
        print("\n4. Demonstrating automatic source selection...")
        
        # Test symbols that exist on different sources
        test_symbols = ['EURUSD', 'BTCUSDT', 'XAUUSD', 'ETHUSDT']
        
        for symbol in test_symbols:
            if data_manager.validate_request(symbol, '1h', 'bars'):
                selected_source = data_manager._select_source(symbol, 'bars')
                print(f"   {symbol}: Will use {selected_source}")
            else:
                print(f"   {symbol}: Not supported")
        
        print("\n💾 Final storage info:")
        print("-" * 25)
        final_info = data_manager.get_stored_info()
        print(final_info.get('summary', 'No stored data'))
        if 'symbols' in final_info:
            print(f"Stored symbols: {final_info['symbols']}")
        
        # Cleanup
        data_manager.close()
        print("\n✅ Example completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 