#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Фреймворк для загрузки и хранения данных Binance US в Colab.
"""

__version__ = '0.1.0'
__author__ = 'AI Developer Team'

from binance_data_framework.api_connector import BinanceUSClient
from binance_data_framework.database_handler import GoogleDriveDataManager
from binance_data_framework.colab_interface import DataDownloaderUI

def launch_ui():
    """
    Инициализирует и отображает DataDownloaderUI в среде Google Colab.

    Эта функция автоматически включает поддержку виджетов Colab,
    создает экземпляры API клиента и менеджера данных по умолчанию,
    и отображает пользовательский интерфейс.

    Returns:
        DataDownloaderUI: Экземпляр созданного и отображенного UI.
    """
    try:
        from google.colab import output
        output.enable_custom_widget_manager()
    except ImportError:
        print("Warning: 'google.colab.output' could not be imported. Custom widget manager not enabled. This UI is designed for Colab.")
    api_client = BinanceUSClient()
    db_manager = GoogleDriveDataManager()
    ui = DataDownloaderUI(api_client=api_client, db_manager=db_manager)
    ui.display()
    return ui
# Контрольные цифры: 12568