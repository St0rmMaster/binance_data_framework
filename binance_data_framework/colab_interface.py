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
from binance_data_framework.database_handler import LocalDataManager
from binance_data_framework.cloud_storage import CloudDataManager, DataSynchronizer


class DataDownloaderUI:
    """
    Класс для создания интерактивного интерфейса для загрузки и отображения данных.
    """
    
    def __init__(self, 
                api_client: BinanceUSClient, 
                db_manager: LocalDataManager, 
                cloud_manager: Optional[CloudDataManager] = None):
        """
        Инициализация интерфейса.
        
        Args:
            api_client: Экземпляр BinanceUSClient для подключения к API
            db_manager: Экземпляр LocalDataManager для работы с базой данных
            cloud_manager: Экземпляр CloudDataManager для работы с облачной базой данных (опционально)
        """
        self.api_client = api_client
        self.db_manager = db_manager
        self.cloud_manager = cloud_manager
        self.data_sync = None
        
        if self.cloud_manager:
            self.data_sync = DataSynchronizer(self.db_manager, self.cloud_manager)
        
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
        
        # Виджет для опции использования только локальных данных
        self.use_local_only_checkbox = widgets.Checkbox(
            value=False,
            description='Только локальные данные',
            indent=False
        )
        
        # Виджет для опции использования облачного хранилища
        self.use_cloud_checkbox = widgets.Checkbox(
            value=False,
            description='Использовать облако',
            indent=False,
            disabled=self.cloud_manager is None
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
            description='Показать локальные данные',
            button_style='info',
            icon='database'
        )
        
        # Добавляем кнопки для синхронизации с облачным хранилищем
        self.sync_to_cloud_button = widgets.Button(
            description='Синхр. в облако',
            button_style='success',
            icon='cloud-upload',
            disabled=self.cloud_manager is None
        )
        
        self.sync_from_cloud_button = widgets.Button(
            description='Синхр. из облака',
            button_style='success',
            icon='cloud-download',
            disabled=self.cloud_manager is None
        )
        
        self.show_cloud_button = widgets.Button(
            description='Показать облачные данные',
            button_style='info',
            icon='cloud',
            disabled=self.cloud_manager is None
        )
        
        # Виджет для отображения графика
        self.plot_checkbox = widgets.Checkbox(
            value=True,
            description='Показать график',
            indent=False
        )
        
        # Виджет для вывода сообщений и результатов
        self.output = widgets.Output()
        
        # Настраиваем обработчики событий
        self.load_button.on_click(self._on_load_button_clicked)
        self.show_local_button.on_click(self._on_show_local_button_clicked)
        self.sync_to_cloud_button.on_click(self._on_sync_to_cloud_clicked)
        self.sync_from_cloud_button.on_click(self._on_sync_from_cloud_clicked)
        self.show_cloud_button.on_click(self._on_show_cloud_button_clicked)
    
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
            
            use_local_only = self.use_local_only_checkbox.value
            use_cloud = self.use_cloud_checkbox.value
            use_resample = self.use_resample_checkbox.value
            plot_data = self.plot_checkbox.value
            
            if end_date < start_date:
                print("Ошибка: Дата окончания должна быть позже даты начала")
                return
            
            print(f"Запрос данных для {symbol} на таймфрейме {timeframe} с {start_date.date()} по {end_date.date()}")
            
            # Получаем данные
            if use_resample and timeframe != '1m':
                # Если выбрано ресемплирование и таймфрейм не 1m
                df = self._get_resampled_data(symbol, timeframe, start_date, end_date, use_local_only, use_cloud)
            else:
                # Стандартная логика получения данных
                df = self._get_data(symbol, timeframe, start_date, end_date, use_local_only, use_cloud)
            
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
        end_date: datetime, 
        use_local_only: bool,
        use_cloud: bool
    ) -> Optional[pd.DataFrame]:
        """
        Получает данные из локальной БД, облака или API.
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
            use_local_only: Флаг использования только локальных данных
            use_cloud: Флаг использования облачного хранилища
            
        Returns:
            pd.DataFrame: DataFrame с данными или None в случае ошибки
        """
        # Проверяем наличие данных в локальной БД
        data_exists, date_range = self.db_manager.check_data_exists(symbol, timeframe, start_date, end_date)
        
        if data_exists:
            print(f"Данные найдены в локальной БД для {symbol} на таймфрейме {timeframe} в указанном периоде")
            df = self.db_manager.get_data(symbol, timeframe, start_date, end_date)
            return df
        
        # Проверяем наличие данных в облачной БД (если разрешено использование облака)
        if use_cloud and self.cloud_manager:
            cloud_data_exists, cloud_date_range = self.cloud_manager.check_data_exists(symbol, timeframe, start_date, end_date)
            
            if cloud_data_exists:
                print(f"Данные найдены в облачной БД для {symbol} на таймфрейме {timeframe} в указанном периоде")
                df = self.cloud_manager.get_data(symbol, timeframe, start_date, end_date)
                
                # Сохраняем полученные из облака данные в локальную БД для кэширования
                print("Сохранение данных из облака в локальную БД...")
                self.db_manager.save_data(df, symbol, timeframe)
                
                return df
        
        if use_local_only:
            print("Выбрана опция 'Только локальные данные', но данные не найдены в БД")
            return None
        
        # Запрашиваем данные из API
        print(f"Загрузка данных из API для {symbol} на таймфрейме {timeframe}")
        df = self.api_client.get_historical_data(symbol, timeframe, start_date, end_date)
        
        if df is not None and not df.empty:
            # Сохраняем полученные данные в локальную БД
            print("Сохранение данных в локальную БД...")
            self.db_manager.save_data(df, symbol, timeframe)
            
            # Если используется облако, сохраняем также в облачную БД
            if use_cloud and self.cloud_manager:
                print("Сохранение данных в облачную БД...")
                self.cloud_manager.save_data(df, symbol, timeframe)
        
        return df
    
    def _get_resampled_data(
        self, 
        symbol: str, 
        target_timeframe: str, 
        start_date: datetime, 
        end_date: datetime, 
        use_local_only: bool,
        use_cloud: bool
    ) -> Optional[pd.DataFrame]:
        """
        Получает данные с минимальным таймфреймом и ресемплирует их до целевого таймфрейма.
        
        Args:
            symbol: Торговая пара
            target_timeframe: Целевой таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
            use_local_only: Флаг использования только локальных данных
            use_cloud: Флаг использования облачного хранилища
            
        Returns:
            pd.DataFrame: Ресемплированный DataFrame с данными или None в случае ошибки
        """
        # Минимальный таймфрейм для загрузки
        base_timeframe = '1m'
        
        print(f"Загрузка данных с таймфреймом {base_timeframe} для последующего ресемплирования до {target_timeframe}")
        
        # Получаем данные с минимальным таймфреймом
        df_base = self._get_data(symbol, base_timeframe, start_date, end_date, use_local_only, use_cloud)
        
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
            
            print("Запрос информации о локальных данных...")
            
            # Получаем информацию о сохраненных данных
            stored_info = self.db_manager.get_stored_info()
            
            if stored_info is None or stored_info.empty:
                print("В локальной базе данных нет сохраненных данных")
                return
            
            print("\nДоступные данные в локальной БД:")
            
            # Выводим информацию в виде таблицы
            display(stored_info)
            
            # Подготавливаем сводку доступных символов и таймфреймов
            unique_symbols = stored_info['symbol'].unique()
            unique_timeframes = stored_info['timeframe'].unique()
            
            print(f"\nВсего уникальных символов: {len(unique_symbols)}")
            print(f"Всего уникальных таймфреймов: {len(unique_timeframes)}")
            
            print("\nСписок символов:")
            print(", ".join(unique_symbols))
            
            print("\nСписок таймфреймов:")
            print(", ".join(unique_timeframes))
    
    def _on_show_cloud_button_clicked(self, button: widgets.Button) -> None:
        """
        Обработчик нажатия кнопки "Показать облачные данные".
        
        Args:
            button: Объект кнопки
        """
        with self.output:
            clear_output()
            
            if not self.cloud_manager:
                print("Облачное хранилище не настроено")
                return
            
            print("Запрос информации о данных в облачном хранилище...")
            
            # Получаем информацию о сохраненных данных в облаке
            stored_info = self.cloud_manager.get_stored_info()
            
            if stored_info is None or stored_info.empty:
                print("В облачной базе данных нет сохраненных данных")
                return
            
            print("\nДоступные данные в облачной БД:")
            
            # Выводим информацию в виде таблицы
            display(stored_info)
            
            # Подготавливаем сводку доступных символов и таймфреймов
            unique_symbols = stored_info['symbol'].unique()
            unique_timeframes = stored_info['timeframe'].unique()
            
            print(f"\nВсего уникальных символов: {len(unique_symbols)}")
            print(f"Всего уникальных таймфреймов: {len(unique_timeframes)}")
            
            print("\nСписок символов:")
            print(", ".join(unique_symbols))
            
            print("\nСписок таймфреймов:")
            print(", ".join(unique_timeframes))
    
    def _on_sync_to_cloud_clicked(self, button: widgets.Button) -> None:
        """
        Обработчик нажатия кнопки "Синхронизировать в облако".
        
        Args:
            button: Объект кнопки
        """
        with self.output:
            clear_output()
            
            if not self.data_sync:
                print("Синхронизатор данных не настроен")
                return
            
            print("Синхронизация данных из локальной БД в облачную...")
            
            # Запускаем синхронизацию
            stats = self.data_sync.sync_local_to_cloud()
            
            print("\nСинхронизация завершена!")
            print(f"Синхронизировано символов: {stats['symbols_synced']}")
            print(f"Синхронизировано записей: {stats['records_synced']}")
            
            if stats['errors'] > 0:
                print(f"Произошло ошибок: {stats['errors']}")
                print("Проверьте логи для получения дополнительной информации.")
    
    def _on_sync_from_cloud_clicked(self, button: widgets.Button) -> None:
        """
        Обработчик нажатия кнопки "Синхронизировать из облака".
        
        Args:
            button: Объект кнопки
        """
        with self.output:
            clear_output()
            
            if not self.data_sync:
                print("Синхронизатор данных не настроен")
                return
            
            print("Синхронизация данных из облачной БД в локальную...")
            
            # Запускаем синхронизацию
            stats = self.data_sync.sync_cloud_to_local()
            
            print("\nСинхронизация завершена!")
            print(f"Синхронизировано символов: {stats['symbols_synced']}")
            print(f"Синхронизировано записей: {stats['records_synced']}")
            
            if stats['errors'] > 0:
                print(f"Произошло ошибок: {stats['errors']}")
                print("Проверьте логи для получения дополнительной информации.")
    
    def display(self) -> None:
        """
        Отображает интерактивный интерфейс в Jupyter/Colab.
        """
        # Создаем контейнеры для группировки виджетов
        symbol_timeframe_container = widgets.HBox([self.symbol_dropdown, self.timeframe_dropdown])
        date_container = widgets.HBox([self.start_date_picker, self.end_date_picker])
        
        options_container = widgets.HBox([
            self.use_local_only_checkbox, 
            self.use_resample_checkbox, 
            self.plot_checkbox
        ])
        
        if self.cloud_manager:
            options_container = widgets.HBox([
                self.use_local_only_checkbox, 
                self.use_cloud_checkbox,
                self.use_resample_checkbox, 
                self.plot_checkbox
            ])
        
        # Основные кнопки
        buttons_container = widgets.HBox([self.load_button, self.show_local_button])
        
        # Если облачное хранилище доступно, добавляем соответствующие кнопки
        cloud_buttons_container = None
        if self.cloud_manager:
            cloud_buttons_container = widgets.HBox([
                self.sync_to_cloud_button,
                self.sync_from_cloud_button,
                self.show_cloud_button
            ])
        
        # Компонуем все виджеты в вертикальный контейнер
        main_components = [
            widgets.HTML("<h2>Загрузчик данных Binance US</h2>"),
            symbol_timeframe_container,
            date_container,
            options_container,
            buttons_container
        ]
        
        if cloud_buttons_container:
            main_components.append(widgets.HTML("<h3>Управление облачным хранилищем</h3>"))
            main_components.append(cloud_buttons_container)
            
        main_components.append(self.output)
        
        main_container = widgets.VBox(main_components)
        
        # Отображаем главный контейнер
        display(main_container)