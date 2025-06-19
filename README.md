# LeviafanDL - Universal Financial Data Framework

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/version-2.0.0-orange.svg)](https://github.com/leviafandl/LeviafanDL)

**LeviafanDL** is a comprehensive Python framework for downloading, processing, and managing financial data from multiple sources. It provides unified access to Forex, cryptocurrency, stock, and commodity data with intelligent caching and flexible storage options.

## 🚀 Key Features

### Multi-Source Data Access
- **Dukascopy**: Forex, CFDs, commodities, indices, and cryptocurrency data with tick-level precision
- **Binance**: Comprehensive cryptocurrency market data
- **Automatic Source Selection**: Intelligent prioritization (Dukascopy preferred, Binance fallback)

### Advanced Data Types
- **OHLCV Bar Data**: Multiple timeframes from 1 minute to 1 month
- **Tick Data**: High-frequency bid/ask data (Dukascopy)
- **Smart Resampling**: Automatic tick-to-bar conversion when beneficial

### Flexible Storage Backends
- **Local Storage**: High-performance DuckDB for local data storage
- **Google Drive**: Cloud storage integration (coming soon)
- **FTP Storage**: Remote server storage (coming soon)

### Environment Adaptability
- **Google Colab**: Automatic detection and integration with Colab secrets
- **Local Development**: `.env` file support for configuration
- **Cross-Platform**: Windows, macOS, and Linux support

## 📦 Installation

### Basic Installation
```bash
pip install leviafan-dl
```

### Development Installation
```bash
git clone https://github.com/leviafandl/LeviafanDL.git
cd LeviafanDL
pip install -e .
```

### Install with Development Tools
```bash
pip install leviafan-dl[dev]
```

## 🔧 Configuration

### Environment Variables (.env file)
Create a `.env` file in your project directory:

```env
# Binance API Configuration
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here

# Local Storage Configuration  
LOCAL_STORAGE_PATH=./data
LOCAL_DB_NAME=leviafan_data.duckdb

# Dukascopy Configuration (optional)
DUKASCOPY_TIMEOUT=30
DUKASCOPY_RETRIES=3
```

### Google Colab Setup
In Google Colab, add your API keys to Colab Secrets:
- `BINANCE_API_KEY`
- `BINANCE_API_SECRET`

## 🎯 Quick Start

### Basic Usage

```python
from leviafan_dl import DataManager
from datetime import datetime, timedelta

# Initialize the data manager
data_manager = DataManager(storage_type='local')

# Define date range
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

# Fetch Forex data (automatically uses Dukascopy)
forex_data = data_manager.fetch_data(
    symbol='EURUSD',
    start_date=start_date,
    end_date=end_date,
    timeframe='1h',
    data_type='bars'
)

# Fetch cryptocurrency data (automatically uses Binance)
crypto_data = data_manager.fetch_data(
    symbol='BTCUSDT', 
    start_date=start_date,
    end_date=end_date,
    timeframe='1h',
    data_type='bars'
)

# Fetch tick data (Dukascopy only)
tick_data = data_manager.fetch_data(
    symbol='EURUSD',
    start_date=datetime.now() - timedelta(hours=2),
    end_date=datetime.now(),
    timeframe='tick',
    data_type='ticks'
)

print(f"Forex data: {len(forex_data)} bars")
print(f"Crypto data: {len(crypto_data)} bars") 
print(f"Tick data: {len(tick_data)} ticks")
```

### Advanced Configuration

```python
from leviafan_dl import DataManager, ConfigManager

# Custom configuration
config_manager = ConfigManager(env_file_path='./custom.env')
data_manager = DataManager(
    storage_type='local',
    config_manager=config_manager
)

# Check available symbols and timeframes
symbols = data_manager.get_available_symbols()
timeframes = data_manager.get_supported_timeframes()

print("Available sources and symbols:")
for source, symbol_list in symbols.items():
    print(f"{source}: {len(symbol_list)} symbols")

# Validate requests before fetching
if data_manager.validate_request('XAUUSD', '1h', 'bars'):
    gold_data = data_manager.fetch_data(
        symbol='XAUUSD',
        start_date=start_date,
        end_date=end_date,
        timeframe='1h'
    )
```

## 📊 Supported Markets

### Dukascopy Data
- **Forex**: 30+ major and minor currency pairs
- **Precious Metals**: Gold, Silver, Platinum, Palladium
- **Commodities**: Oil, Coffee, Corn, Sugar, Wheat, Natural Gas
- **Indices**: S&P 500, Dow Jones, DAX, FTSE, Nikkei
- **Cryptocurrencies**: 18 major crypto pairs
- **Stocks (CFDs)**: Major US and European stocks

### Binance Data
- **Cryptocurrencies**: All active trading pairs on Binance US
- **Timeframes**: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M

## 🔄 Data Processing Features

### Intelligent Caching
- Automatic detection of existing data
- Incremental updates for missing date ranges
- Efficient storage with DuckDB backend

### Tick-to-Bar Resampling
```python
# Framework automatically resamples tick data when beneficial
bar_data = data_manager.fetch_data(
    symbol='EURUSD',
    start_date=start_date,
    end_date=end_date,
    timeframe='5m',  # Will resample from ticks if available
    data_type='bars'
)
```

### Data Validation
- Automatic DataFrame structure validation
- Missing data detection and handling
- Data type consistency checks

## 📁 Storage Management

### View Stored Data
```python
# Get storage information
info = data_manager.get_stored_info()
print(f"Summary: {info['summary']}")
print(f"Symbols: {info['symbols']}")
print(f"Timeframes: {info['timeframes']}")
```

### Data Cleanup
```python
# Delete specific data
data_manager.delete_data('BTCUSDT', '1h', 'bars')

# Close connections
data_manager.close()
```

## 🎮 Google Colab Integration

LeviafanDL works seamlessly in Google Colab with automatic environment detection:

```python
# In Google Colab - no additional setup needed
from leviafan_dl import DataManager

# Automatically detects Colab environment and uses Colab secrets
data_manager = DataManager()

# Your data fetching code works the same way
data = data_manager.fetch_data('EURUSD', start_date, end_date, '1h')
```

## 🛠️ Development

### Project Structure
```
LeviafanDL/
├── leviafan_dl/
│   ├── __init__.py
│   ├── sources/          # Data source implementations
│   │   ├── base_source.py
│   │   ├── dukascopy_source.py
│   │   └── binance_source.py
│   ├── storage/          # Storage backend implementations
│   │   ├── base_storage.py
│   │   ├── local_storage.py
│   │   ├── gdrive_storage.py (coming soon)
│   │   └── ftp_storage.py (coming soon)
│   ├── core/            # Core orchestration
│   │   ├── config_manager.py
│   │   └── data_manager.py
│   └── ui/              # User interfaces
│       └── colab_interface.py
├── examples/            # Usage examples
├── requirements.txt
└── setup.py
```

### Running Examples
```bash
# Basic usage example
python examples/basic_usage.py

# Advanced features example  
python examples/advanced_usage.py
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Support

- **Documentation**: [Coming Soon]
- **Issues**: [GitHub Issues](https://github.com/leviafandl/LeviafanDL/issues)
- **Discussions**: [GitHub Discussions](https://github.com/leviafandl/LeviafanDL/discussions)

## 🚧 Roadmap

### Version 2.1.0
- [ ] Google Drive storage backend
- [ ] FTP storage backend
- [ ] Interactive Jupyter widgets UI
- [ ] Data export to multiple formats

### Version 2.2.0
- [ ] Additional data sources (Alpha Vantage, Yahoo Finance)
- [ ] Real-time data streaming
- [ ] Advanced data analysis tools
- [ ] Performance optimizations

## ⚠️ Disclaimers

- **Data Usage**: Historical data is provided for research and educational purposes
- **API Limits**: Respect rate limits of data providers
- **Market Data**: No guarantees on data accuracy or completeness
- **Trading**: This framework is for analysis only, not trading advice

---

**LeviafanDL** - Making financial data access simple and unified! 🚀