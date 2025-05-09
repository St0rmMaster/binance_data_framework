#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для создания интерактивного интерфейса в Google Colab.
"""

import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from typing import Optional, List, Dict, Any, Tuple, Union

from binance_data_framework.api_connector import BinanceUSClient
from binance_data_framework.database_handler import GoogleDriveDataManager


class DataDownloaderUI:
    """
    Класс для создания интерактивного интерфейса для загрузки и отображения данных.
    """
    
    def __init__(self, api_client: BinanceUSClient, db_manager: GoogleDriveDataManager):
        """
        Инициализация интерфейса.
        
        Args:
            api_client: Экземпляр BinanceUSClient для подключения к API
            db_manager: Экземпляр GoogleDriveDataManager для работы с базой данных на Google Drive
        """
        self.api_client = api_client
        self.db_manager = db_manager
        
        # Инициализация виджетов
        self.symbols = []
        self.timeframes = []
        self._fetch_initial_data()
        self._create_widgets()
    
    def _fetch_initial_data(self) -> None:
        """
        Получает список доступных USDT-пар и таймфреймов.
        """
        # Получаем список USDT-пар
        self.symbols = self.api_client.get_usdt_trading_pairs()
        if not self.symbols:
            self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "XRPUSDT"]  # Значения по умолчанию
            print("Не удалось получить список торговых пар. Используем значения по умолчанию.")
        
        # Получаем список таймфреймов
        self.timeframes = self.api_client.get_available_intervals()
        if not self.timeframes:
            self.timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]  # Значения по умолчанию
            print("Не удалось получить список таймфреймов. Используем значения по умолчанию.")
    
    def _create_widgets(self) -> None:
        """
        Создает виджеты для интерактивного интерфейса.
        """
        # Виджет выбора символа (торговой пары)
        self.symbol_dropdown = widgets.Dropdown(
            options=self.symbols,
            value=self.symbols[0] if self.symbols else "BTCUSDT",
            description='Торговая пара:',
            style={'description_width': 'initial'},
            layout=widgets.Layout(width='50%')
        )
        
        # Виджет выбора таймфрейма
        self.timeframe_dropdown = widgets.Dropdown(
            options=self.timeframes,
            value='1h',
            description='Таймфрейм:',
            style={'description_width': 'initial'},
            layout=widgets.Layout(width='50%')
        )
        
        # Виджеты для выбора даты начала и конца периода
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)  # По умолчанию 30 дней
        
        self.start_date_picker = widgets.DatePicker(
            description='Дата начала:',
            value=start_date.date(),
            style={'description_width': 'initial'}
        )
        
        self.end_date_picker = widgets.DatePicker(
            description='Дата конца:',
            value=end_date.date(),
            style={'description_width': 'initial'}
        )
          # Виджет для опции загрузки минимального таймфрейма и ресемплирования
        self.use_resample_checkbox = widgets.Checkbox(
            value=False,
            description='Ресемплировать из мин. таймфрейма',
            indent=False,
            layout=widgets.Layout(width='50%')
        )
        
        # Кнопки для загрузки и отображения данных
        self.load_button = widgets.Button(
            description='Загрузить данные',
            button_style='primary',
            icon='download'
        )
        self.show_local_button = widgets.Button(
            description='Данные на Диске',
            button_style='info',
            icon='cloud-download'
        )
        
        # Виджет для отображения графика
        self.plot_checkbox = widgets.Checkbox(
            value=False,  # По умолчанию график не показывается
            description='Показать график',
            indent=False
        )
        
        # Виджет для вывода сообщений и результатов
        self.output = widgets.Output()
        
        # Настраиваем обработчики событий
        self.load_button.on_click(self._on_load_button_clicked)
        self.show_local_button.on_click(self._on_show_local_button_clicked)
    
    def _on_load_button_clicked(self, button: widgets.Button) -> None:
        """
        Обработчик нажатия кнопки "Загрузить данные".
        
        Args:
            button: Объект кнопки
        """
        with self.output:
            clear_output()
            
            # Получаем выбранные значения из виджетов
            symbol = self.symbol_dropdown.value
            timeframe = self.timeframe_dropdown.value
            start_date = datetime.combine(self.start_date_picker.value, datetime.min.time())
            end_date = datetime.combine(self.end_date_picker.value, datetime.max.time())
            
            use_resample = self.use_resample_checkbox.value
            plot_data = self.plot_checkbox.value
            
            if end_date < start_date:
                print("Ошибка: Дата окончания должна быть позже даты начала")
                return
            
            print(f"Запрос данных для {symbol} на таймфрейме {timeframe} с {start_date.date()} по {end_date.date()}")
              # Получаем данные
            if use_resample and timeframe != '1m':
                # Если выбрано ресемплирование и таймфрейм не 1m
                df = self._get_resampled_data(symbol, timeframe, start_date, end_date)
            else:
                # Стандартная логика получения данных
                df = self._get_data(symbol, timeframe, start_date, end_date)
            
            if df is not None and not df.empty:
                print(f"\nПолучено {len(df)} строк данных")
                print("\nПервые 5 строк:")
                display(df.head())
                
                print("\nПоследние 5 строк:")
                display(df.tail())
                
                print("\nИнформация о данных:")
                df.info()
                
                # Отображаем график, если выбрана соответствующая опция
                if plot_data:
                    self._plot_data(df, symbol, timeframe)
            else:
                print("Данные не получены")
    def _get_data(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Получает данные из БД на Google Drive или API.
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
            
        Returns:
            pd.DataFrame: DataFrame с данными или None в случае ошибки
        """
        # Диагностические сообщения перед проверкой наличия данных
        print(f"\n[DEBUG _get_data] Вызов check_data_exists для: symbol={symbol}, timeframe={timeframe}")
        print(f"[DEBUG _get_data] Период: start_date={start_date}, end_date={end_date}")
        
        # Проверяем наличие данных в БД на Google Drive
        data_exists, date_range = self.db_manager.check_data_exists(symbol, timeframe, start_date, end_date)
        
        # Диагностические сообщения после проверки
        print(f"[DEBUG _get_data] Результат check_data_exists: data_exists={data_exists}, date_range={date_range}")
        
        if data_exists:
            print(f"Данные найдены в БД на Google Drive для {symbol} на таймфрейме {timeframe} в указанном периоде")
            df = self.db_manager.get_data(symbol, timeframe, start_date, end_date)
            return df
        
        # Запрашиваем данные из API
        print(f"Загрузка данных из API для {symbol} на таймфрейме {timeframe}")
        df = self.api_client.get_historical_data(symbol, timeframe, start_date, end_date)
        
        if df is not None and not df.empty:
            # Сохраняем полученные данные в БД
            print("Сохранение данных в БД на Google Drive...")
            self.db_manager.save_data(df, symbol, timeframe)
        
        return df
    def _get_resampled_data(
        self, 
        symbol: str, 
        target_timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Получает данные с минимальным таймфреймом и ресемплирует их до целевого таймфрейма.
        
        Args:
            symbol: Торговая пара
            target_timeframe: Целевой таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
            use_local_only: Флаг использования только локальных данных
            
        Returns:
            pd.DataFrame: Ресемплированный DataFrame с данными или None в случае ошибки
        """
        # Минимальный таймфрейм для загрузки
        base_timeframe = '1m'
        
        print(f"Загрузка данных с таймфреймом {base_timeframe} для последующего ресемплирования до {target_timeframe}")
        
        # Получаем данные с минимальным таймфреймом
        df_base = self._get_data(symbol, base_timeframe, start_date, end_date)
        
        if df_base is None or df_base.empty:
            print(f"Не удалось получить базовые данные с таймфреймом {base_timeframe}")
            return None
        
        # Ресемплируем данные до целевого таймфрейма
        print(f"Ресемплирование данных из {base_timeframe} в {target_timeframe}")
        
        # Преобразуем строковое представление таймфрейма в формат для ресемплирования
        resampling_rule = self._convert_timeframe_to_rule(target_timeframe)
        
        if not resampling_rule:
            print(f"Не удалось определить правило ресемплирования для таймфрейма {target_timeframe}")
            return None
        
        try:
            # Ресемплируем данные
            df_resampled = pd.DataFrame()
            df_resampled['open'] = df_base['open'].resample(resampling_rule).first()
            df_resampled['high'] = df_base['high'].resample(resampling_rule).max()
            df_resampled['low'] = df_base['low'].resample(resampling_rule).min()
            df_resampled['close'] = df_base['close'].resample(resampling_rule).last()
            df_resampled['volume'] = df_base['volume'].resample(resampling_rule).sum()
            
            # Убираем строки с NaN значениями
            df_resampled.dropna(inplace=True)
            
            print(f"Ресемплирование завершено. Получено {len(df_resampled)} строк данных.")
            
            return df_resampled
            
        except Exception as e:
            print(f"Ошибка при ресемплировании данных: {e}")
            return None
    
    def _convert_timeframe_to_rule(self, timeframe: str) -> Optional[str]:
        """
        Преобразует строковое представление таймфрейма в правило для ресемплирования pandas.
        
        Args:
            timeframe: Строковое представление таймфрейма (например, '1h', '1d')
            
        Returns:
            Optional[str]: Правило для ресемплирования или None, если не удалось преобразовать
        """
        # Словарь преобразования таймфреймов Binance в правила ресемплирования pandas
        timeframe_rules = {
            '1m': '1min',
            '3m': '3min',
            '5m': '5min',
            '15m': '15min',
            '30m': '30min',
            '1h': '1H',
            '2h': '2H',
            '4h': '4H',
            '6h': '6H',
            '8h': '8H',
            '12h': '12H',
            '1d': '1D',
            '3d': '3D',
            '1w': '1W',
            '1M': '1M'
        }
        
        return timeframe_rules.get(timeframe)
    
    def _plot_data(self, df: pd.DataFrame, symbol: str, timeframe: str) -> None:
        """
        Отображает график OHLCV данных.
        
        Args:
            df: DataFrame с данными
            symbol: Торговая пара
            timeframe: Таймфрейм
        """
        try:
            plt.figure(figsize=(12, 8))
            
            # График цены
            ax1 = plt.subplot(2, 1, 1)
            ax1.plot(df.index, df['close'], label='close')
            ax1.set_title(f'{symbol} - {timeframe}')
            ax1.set_ylabel('Цена')
            ax1.legend()
            ax1.grid(True)
            
            # График объема
            ax2 = plt.subplot(2, 1, 2, sharex=ax1)
            ax2.bar(df.index, df['volume'], label='volume', alpha=0.7)
            ax2.set_xlabel('Дата')
            ax2.set_ylabel('Объем')
            ax2.legend()
            ax2.grid(True)
            
            plt.tight_layout()
            plt.show()
            
        except Exception as e:
            print(f"Ошибка при построении графика: {e}")
    
    def _on_show_local_button_clicked(self, button: widgets.Button) -> None:
        """
        Обработчик нажатия кнопки "Показать локальные данные".
        
        Args:
            button: Объект кнопки
        """
        with self.output:
            clear_output()
            print("Запрос информации о данных на Google Drive...")
            stored_info = self.db_manager.get_stored_info()
            if stored_info is None or stored_info.empty:
                print("В БД на Google Drive нет сохраненных данных")
                return
            print("\nДоступные данные в БД на Google Drive:")
            # Выводим только читаемые даты и основные поля
            display(stored_info[['symbol', 'timeframe', 'start_date', 'end_date']])
            unique_symbols = stored_info['symbol'].unique()
            unique_timeframes = stored_info['timeframe'].unique()
            print(f"\nВсего уникальных символов: {len(unique_symbols)}")
            print(f"Всего уникальных таймфреймов: {len(unique_timeframes)}")
            print("\nСписок символов:")
            print(", ".join(unique_symbols))
            print("\nСписок таймфреймов:")
            print(", ".join(unique_timeframes))
    
    def display(self) -> None:
        """
        Отображает интерактивный интерфейс в Jupyter/Colab.
        """
        # Создаем контейнеры для группировки виджетов
        symbol_timeframe_container = widgets.HBox([self.symbol_dropdown, self.timeframe_dropdown])
        date_container = widgets.HBox([self.start_date_picker, self.end_date_picker])
        options_container = widgets.HBox([self.use_resample_checkbox, self.plot_checkbox])
        buttons_container = widgets.HBox([self.load_button, self.show_local_button])
        
        # Компонуем все виджеты в вертикальный контейнер
        main_container = widgets.VBox([
            widgets.HTML("<h2>Загрузчик данных Binance US</h2>"),
            symbol_timeframe_container,
            date_container,
            options_container,
            buttons_container,
            self.output
        ])
        
        # Отображаем главный контейнер
        display(main_container)