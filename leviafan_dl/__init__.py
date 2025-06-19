"""
LeviafanDL - Universal Financial Data Framework

A comprehensive framework for downloading and managing financial data from multiple sources
including Dukascopy (Forex, CFDs, commodities) and Binance (cryptocurrencies).

Supports multiple storage backends: local (DuckDB), Google Drive, and FTP.
Automatically adapts to execution environment (Google Colab or local Python).
"""

__version__ = "2.0.0"
__author__ = "LeviafanDL Team"

# Main classes for easy import
from .core.data_manager import DataManager
from .core.config_manager import ConfigManager

# UI components
from .ui.colab_interface import DataDownloaderUI

# Make main classes available at package level
__all__ = [
    "DataManager",
    "ConfigManager", 
    "DataDownloaderUI"
] 