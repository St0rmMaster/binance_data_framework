"""
Configuration and secrets management for LeviafanDL.

Automatically detects execution environment (Google Colab vs local Python)
and loads secrets from appropriate sources.
"""

import os
import sys
from typing import Optional, Dict, Any
from pathlib import Path


class ConfigManager:
    """
    Manages configuration and secrets for LeviafanDL framework.
    
    Automatically detects execution environment and loads secrets from:
    - Google Colab: google.colab.userdata
    - Local environment: .env file using python-dotenv
    """
    
    def __init__(self, env_file_path: Optional[str] = None):
        """
        Initialize ConfigManager.
        
        Args:
            env_file_path: Path to .env file for local environment.
                          If None, looks for .env in current directory.
        """
        self.is_colab = self._detect_colab()
        self.env_file_path = env_file_path or ".env"
        self._setup_environment()
    
    def _detect_colab(self) -> bool:
        """Detect if running in Google Colab environment."""
        return 'google.colab' in sys.modules
    
    def _setup_environment(self):
        """Setup environment based on detected platform."""
        if not self.is_colab:
            # Load .env file for local environment
            try:
                from dotenv import load_dotenv
                if Path(self.env_file_path).exists():
                    load_dotenv(self.env_file_path)
                    print(f"✓ Loaded environment from {self.env_file_path}")
                else:
                    print(f"⚠ Environment file {self.env_file_path} not found")
            except ImportError:
                print("⚠ python-dotenv not installed. Install it to use .env files.")
        else:
            print("✓ Running in Google Colab environment")
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get secret value from appropriate source.
        
        Args:
            key: Secret key name
            default: Default value if secret not found
            
        Returns:
            Secret value or default
        """
        if self.is_colab:
            try:
                from google.colab import userdata
                return userdata.get(key)
            except Exception as e:
                print(f"⚠ Failed to get secret '{key}' from Colab userdata: {e}")
                return default
        else:
            return os.getenv(key, default)
    
    def get_storage_config(self, storage_type: str) -> Dict[str, Any]:
        """
        Get storage configuration based on storage type.
        
        Args:
            storage_type: Type of storage ('local', 'gdrive', 'ftp')
            
        Returns:
            Dictionary with storage configuration
        """
        if storage_type == 'local':
            return {
                'type': 'local',
                'path': self.get_secret('LOCAL_STORAGE_PATH', './data'),
                'db_name': self.get_secret('LOCAL_DB_NAME', 'leviafan_data.duckdb')
            }
        elif storage_type == 'gdrive':
            return {
                'type': 'gdrive',
                'folder_id': self.get_secret('GDRIVE_FOLDER_ID'),
                'credentials_path': self.get_secret('GDRIVE_CREDENTIALS_PATH'),
                'token_path': self.get_secret('GDRIVE_TOKEN_PATH', 'token.json')
            }
        elif storage_type == 'ftp':
            return {
                'type': 'ftp',
                'host': self.get_secret('FTP_HOST'),
                'port': int(self.get_secret('FTP_PORT') or '21'),
                'username': self.get_secret('FTP_USERNAME'),
                'password': self.get_secret('FTP_PASSWORD'),
                'remote_path': self.get_secret('FTP_REMOTE_PATH', '/data')
            }
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")
    
    def get_api_config(self, source: str) -> Dict[str, Any]:
        """
        Get API configuration for data sources.
        
        Args:
            source: Data source name ('binance', 'dukascopy')
            
        Returns:
            Dictionary with API configuration
        """
        if source == 'binance':
            return {
                'api_key': self.get_secret('BINANCE_API_KEY'),
                'api_secret': self.get_secret('BINANCE_API_SECRET'),
                'testnet': (self.get_secret('BINANCE_TESTNET', 'false') or 'false').lower() == 'true'
            }
        elif source == 'dukascopy':
            # Dukascopy doesn't require API keys for historical data
            return {
                'timeout': int(self.get_secret('DUKASCOPY_TIMEOUT') or '30'),
                'retries': int(self.get_secret('DUKASCOPY_RETRIES') or '3')
            }
        else:
            raise ValueError(f"Unknown data source: {source}")
    
    def validate_config(self, storage_type: str, required_sources: Optional[list] = None) -> bool:
        """
        Validate configuration for given storage type and data sources.
        
        Args:
            storage_type: Storage type to validate
            required_sources: List of required data sources
            
        Returns:
            True if configuration is valid
        """
        try:
            storage_config = self.get_storage_config(storage_type)
            
            # Validate storage config
            if storage_type == 'gdrive' and not storage_config.get('folder_id'):
                print("⚠ Google Drive folder ID not configured")
                return False
            elif storage_type == 'ftp' and not storage_config.get('host'):
                print("⚠ FTP host not configured")
                return False
            
            # Validate API configs
            if required_sources:
                for source in required_sources:
                    if source == 'binance':
                        api_config = self.get_api_config('binance')
                        if not api_config.get('api_key') or not api_config.get('api_secret'):
                            print("⚠ Binance API credentials not configured")
                            return False
            
            return True
            
        except Exception as e:
            print(f"⚠ Configuration validation failed: {e}")
            return False 