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
import os

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
        self.last_loaded_data_params = {}
        
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
        # --- Новый блок выбора символов ---
        self.symbol_filter_input = widgets.Text(
            value='',
            description='Фильтр символов:',
            style={'description_width': 'initial'},
            layout=widgets.Layout(width='100%')
        )
        self.select_all_symbols_checkbox = widgets.Checkbox(
            value=False,
            description='Выбрать/Снять все видимые символы',
            indent=False
        )
        self.symbol_checkboxes_container = widgets.VBox(
            [],
            layout=widgets.Layout(
                max_height='300px',
                overflow_y='auto',
                border='1px solid lightgray',
                padding='5px',
                width='100%'
            )
        )
        # Словарь всех чекбоксов для символов
        self.all_symbol_checkbox_widgets = {
            symbol: widgets.Checkbox(
                description=symbol,
                value=False,
                indent=False,
                layout=widgets.Layout(width='auto')
            ) for symbol in self.symbols
        }
        # --- Таймфрейм и остальные виджеты ---
        self.timeframe_dropdown = widgets.Dropdown(
            options=self.timeframes,
            value='1h',
            description='Таймфрейм:',
            style={'description_width': 'initial'},
            layout=widgets.Layout(width='50%')
        )
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
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
        self.use_resample_checkbox = widgets.Checkbox(
            value=False,
            description='Ресемплировать из мин. таймфрейма',
            indent=False,
            layout=widgets.Layout(width='50%')
        )
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
        self.plot_checkbox = widgets.Checkbox(
            value=False,
            description='Показать график',
            indent=False
        )
        self.output = widgets.Output()
        # --- Виджеты для удаления данных ---
        self.delete_symbol_input = widgets.Text(description='Символ для удаления:', layout=widgets.Layout(width='auto'))
        self.delete_timeframe_input = widgets.Text(description='Таймфрейм для удаления:', layout=widgets.Layout(width='auto'))
        self.confirm_delete_checkbox = widgets.Checkbox(description='Я подтверждаю удаление этих данных', value=False, indent=False)
        self.delete_data_button = widgets.Button(description='Удалить данные из БД', button_style='danger', icon='trash')
        self.delete_data_button.on_click(self._on_delete_data_button_clicked)
        # --- Виджеты для экспорта данных ---
        self.export_format_dropdown = widgets.Dropdown(options=['CSV', 'Parquet'], value='CSV', description='Формат экспорта:', layout=widgets.Layout(width='auto'))
        # Не добавляем self.export_data_button в интерфейс, но оставляем инициализацию для обратной совместимости
        # self.export_data_button = widgets.Button(description='Экспортировать загруженные данные', button_style='success', icon='save')
        # self.export_data_button.on_click(self._on_export_data_button_clicked)
        # --- Прогресс-бар ---
        self.progress_bar = widgets.FloatProgress(
            value=0.0,
            min=0.0,
            max=1.0,
            description='Прогресс:',
            bar_style='info',
            orientation='horizontal',
            layout=widgets.Layout(width='280px', height='16px', margin='5px 0', visibility='hidden')
        )
        # --- Привязка обработчиков ---
        self.load_button.on_click(self._on_load_button_clicked)
        self.show_local_button.on_click(self._on_show_local_button_clicked)
        self.symbol_filter_input.observe(self._update_visible_symbol_checkboxes, names='value')
        self.select_all_symbols_checkbox.observe(self._on_select_all_toggled, names='value')
        # --- Инициализация видимых чекбоксов ---
        self._update_visible_symbol_checkboxes()

        # Обновление layout для компактности и двухколоночного вида
        self.timeframe_dropdown.layout = widgets.Layout(width='280px', margin='0 0 8px 0')
        self.symbol_filter_input.layout = widgets.Layout(width='280px', margin='0 0 8px 0')
        self.select_all_symbols_checkbox.layout = widgets.Layout(width='280px', margin='0 0 5px 0')
        self.symbol_checkboxes_container.layout = widgets.Layout(
            width='280px', max_height='250px', overflow_y='auto', border='1px solid lightgray', padding='5px', margin='0 0 8px 0')
        self.start_date_picker.layout = widgets.Layout(width='280px', margin='0 0 8px 0')
        self.end_date_picker.layout = widgets.Layout(width='280px', margin='0 0 8px 0')
        self.use_resample_checkbox.layout = widgets.Layout(width='280px', margin='0 0 8px 0')
        self.plot_checkbox.layout = widgets.Layout(width='280px', margin='0 0 8px 0')
        self.load_button.layout = widgets.Layout(width='280px', margin='5px 0')
        self.show_local_button.layout = widgets.Layout(width='280px', margin='5px 0')
        # self.export_data_button.layout = widgets.Layout(width='280px', margin='5px 0')
        # Для правой колонки (локальные данные)
        self.local_data_management_area = widgets.VBox([], layout=widgets.Layout(width='auto', padding='10px', align_items='flex-start'))

    def _update_visible_symbol_checkboxes(self, change=None):
        """
        Обновляет список видимых чекбоксов символов согласно фильтру.
        """
        filter_text = self.symbol_filter_input.value.strip().lower()
        visible_checkboxes = []
        for symbol, cb_widget in self.all_symbol_checkbox_widgets.items():
            if not filter_text or filter_text in symbol.lower():
                visible_checkboxes.append(cb_widget)
        self.symbol_checkboxes_container.children = tuple(visible_checkboxes)

    def _on_select_all_toggled(self, change):
        """
        Обработчик для чекбокса "выбрать/снять все видимые символы".
        """
        new_value = self.select_all_symbols_checkbox.value
        for cb_widget in self.symbol_checkboxes_container.children:
            cb_widget.value = new_value

    def _on_load_button_clicked(self, button: widgets.Button) -> None:
        selected_symbols = [
            symbol for symbol, cb_widget in self.all_symbol_checkbox_widgets.items()
            if cb_widget.value
        ]
        with self.output:
            clear_output(wait=True)
            num_symbols = len(selected_symbols)
            if (num_symbols > 0):
                self.progress_bar.value = 0.0
                self.progress_bar.max = float(num_symbols)
                self.progress_bar.layout.visibility = 'visible'
            else:
                self.progress_bar.layout.visibility = 'hidden'
            if not selected_symbols:
                print("Ошибка: Ни один символ не выбран.")
                return
            timeframe = self.timeframe_dropdown.value
            start_date = datetime.combine(self.start_date_picker.value, datetime.min.time())
            end_date = datetime.combine(self.end_date_picker.value, datetime.max.time())
            use_resample = self.use_resample_checkbox.value
            plot_data = self.plot_checkbox.value
            if end_date < start_date:
                print("Ошибка: Дата окончания должна быть позже даты начала")
                self.progress_bar.layout.visibility = 'hidden'
                return
            loaded_dataframes = {}
            summary = []
            for idx, symbol in enumerate(selected_symbols):
                try:
                    if use_resample and timeframe != '1m':
                        df = self._get_resampled_data(symbol, timeframe, start_date, end_date)
                    else:
                        df = self._get_data(symbol, timeframe, start_date, end_date)
                    if df is not None and not df.empty:
                        loaded_dataframes[symbol] = df
                        summary.append(f"{symbol} — {len(df)} строк")
                        if plot_data:
                            self._plot_data(df, symbol, timeframe)
                    else:
                        summary.append(f"{symbol} — нет данных")
                except Exception as e:
                    summary.append(f"{symbol} — ошибка: {e}")
                self.progress_bar.value = (idx + 1) / num_symbols
            self.progress_bar.layout.visibility = 'hidden'
            if loaded_dataframes:
                self.last_loaded_data_params = {
                    'timeframe': timeframe,
                    'start_date': start_date,
                    'end_date': end_date,
                    'dataframes': loaded_dataframes
                }
            else:
                self.last_loaded_data_params = {}
            if summary:
                print("Загружено:")
                print("; ".join(summary))

    def _on_delete_data_button_clicked(self, button):
        symbol = self.delete_symbol_input.value.strip()
        timeframe = self.delete_timeframe_input.value.strip()
        with self.output:
            clear_output(wait=True)
            if not symbol or not timeframe:
                print("Пожалуйста, укажите символ и таймфрейм для удаления.")
                return
            if not self.confirm_delete_checkbox.value:
                print("Пожалуйста, подтвердите удаление.")
                return
            result = self.db_manager.delete_data(symbol, timeframe)
            if result:
                print(f"Данные для {symbol}/{timeframe} успешно удалены.")
            else:
                print(f"Ошибка при удалении данных для {symbol}/{timeframe}.")
            self.confirm_delete_checkbox.value = False
            print("Для обновления информации нажмите 'Данные на Диске'.")

    def _on_export_data_button_clicked(self, button):
        with self.output:
            clear_output(wait=True)
            params = self.last_loaded_data_params
            if not params or 'dataframes' not in params or not params['dataframes']:
                print("Нет данных для экспорта. Сначала загрузите данные.")
                return
            export_format = self.export_format_dropdown.value
            timeframe = params.get('timeframe')
            start_date = params.get('start_date')
            end_date = params.get('end_date')
            exports_dir = os.path.join(self.db_manager.db_directory, 'exports')
            os.makedirs(exports_dir, exist_ok=True)
            for symbol, df in params['dataframes'].items():
                filename = f"{symbol}_{timeframe}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.{export_format.lower()}"
                filepath = os.path.join(exports_dir, filename)
                try:
                    if export_format == 'CSV':
                        df.to_csv(filepath, index=True)
                    elif export_format == 'Parquet':
                        df.to_parquet(filepath, index=True)
                    print(f"Экспортировано: {filepath}")
                except Exception as e:
                    print(f"Ошибка экспорта {symbol}: {e}")

    def display(self) -> None:
        clear_output(wait=True)
        # Левая колонка: загрузка новых данных
        left_column = widgets.VBox([
            widgets.HTML("<h4>Параметры загрузки:</h4>"),
            self.timeframe_dropdown,
            self.symbol_filter_input,
            self.select_all_symbols_checkbox,
            self.symbol_checkboxes_container,
            widgets.HTML("<h4>Период и опции:</h4>"),
            self.start_date_picker,
            self.end_date_picker,
            self.use_resample_checkbox,
            self.plot_checkbox,
            self.load_button,
            self.progress_bar
        ], layout=widgets.Layout(width='auto', padding='10px', align_items='flex-start'))

        # Правая колонка: управление локальными данными (заполняется динамически)
        if not hasattr(self, 'local_data_management_area'):
            self.local_data_management_area = widgets.VBox([], layout=widgets.Layout(width='auto', padding='10px', align_items='flex-start'))

        right_column = widgets.VBox([
            self.local_data_management_area
        ], layout=widgets.Layout(width='420px', padding='10px', align_items='flex-start'))

        main_controls_layout = widgets.HBox([
            left_column,
            right_column
        ])

        main_container = widgets.VBox([
            widgets.HTML("<h2>Загрузчик данных Binance US</h2>"),
            main_controls_layout,
            self.output
        ])
        display(main_container)

        # Автоматически загружаем данные при открытии UI
        self._on_show_local_button_clicked(None)

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
        data_exists, date_range = self.db_manager.check_data_exists(symbol, timeframe, start_date, end_date)
        if data_exists:
            print(f"Данные найдены в БД для {symbol} {timeframe}")
            df = self.db_manager.get_data(symbol, timeframe, start_date, end_date)
            return df
        print(f"Загрузка из API: {symbol} {timeframe}")
        df = self.api_client.get_historical_data(symbol, timeframe, start_date, end_date)
        if df is not None and not df.empty:
            print("Сохранение в БД...")
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
    
    def _on_show_local_button_clicked(self, button) -> None:
        """
        Обработчик для кнопки "Данные на Диске". Отображает список доступных данных с чекбоксами в правой колонке.
        """
        with self.output:
            clear_output(wait=True)
        # Очистить правую колонку
        self.local_data_management_area.children = []
        stored_info = self.db_manager.get_stored_info()
        self.current_stored_info = stored_info  # Для экспорта/удаления
        if stored_info.empty:
            with self.output:
                print("Нет данных на Google Drive.")
            return
        # Заголовок
        right_header = widgets.HTML("<h4>Данные на Google Drive:</h4>")
        # Прокручиваемый список чекбоксов
        local_data_items_container = widgets.VBox(layout=widgets.Layout(
            width='100%', min_width='480px', max_width='none', max_height='400px',
            overflow_y='auto', overflow_x='hidden', border='1px solid lightgray',
            padding='5px', margin='0 0 10px 0', box_sizing='border-box', display='block',
            flex_flow='column', flex_wrap='nowrap',
        ))
        self.local_data_checkboxes = {}
        checkboxes = []
        for _, row in stored_info.iterrows():
            symbol, timeframe, start_date, end_date = row['symbol'], row['timeframe'], row['start_date'], row['end_date']
            cb = widgets.Checkbox(
                description=f"{symbol} - {timeframe} (с {start_date} по {end_date})",
                value=False,
                indent=False
            )
            self.local_data_checkboxes[(symbol, timeframe)] = cb
            checkboxes.append(cb)
        local_data_items_container.children = tuple(checkboxes)
        # Кнопки и чекбокс подтверждения
        self.export_local_csv_button = widgets.Button(description='Экспорт в CSV', icon='file-excel', layout=widgets.Layout(width='auto', margin='0 5px 0 0'))
        self.export_local_parquet_button = widgets.Button(description='Экспорт в Parquet', icon='file-archive', layout=widgets.Layout(width='auto', margin='0 5px 0 0'))
        self.load_as_current_df_button = widgets.Button(description='Загрузить как текущий датафрейм', icon='table', layout=widgets.Layout(width='auto', margin='0 5px 0 0'))
        self.delete_local_selected_button = widgets.Button(description='Удалить выбранное', button_style='danger', icon='trash', layout=widgets.Layout(width='auto'))
        self.confirm_delete_local_list_checkbox = widgets.Checkbox(description='Подтверждаю удаление выбранного из списка', value=False, indent=False, layout=widgets.Layout(margin='5px 0'))
        # Привязка обработчиков
        self.export_local_csv_button.on_click(lambda b: self._on_export_local_data_clicked(b, export_format='CSV'))
        self.export_local_parquet_button.on_click(lambda b: self._on_export_local_data_clicked(b, export_format='Parquet'))
        self.load_as_current_df_button.on_click(self._on_load_as_current_df_clicked)
        self.delete_local_selected_button.on_click(self._on_delete_local_selected_from_list_clicked)
        action_buttons_for_local_data = widgets.VBox([
            widgets.HBox([self.export_local_csv_button, self.export_local_parquet_button]),
            widgets.HBox([self.load_as_current_df_button]),
            widgets.HBox([self.delete_local_selected_button, self.confirm_delete_local_list_checkbox])
        ])
        # Обновить правую колонку
        self.local_data_management_area.children = (right_header, local_data_items_container, action_buttons_for_local_data)

        # Удаляем горизонтальный скролл у всего интерфейса (VBox/HBox)
        # Применяем к main_container после display
        import time
        time.sleep(0.1)  # Дать время на отрисовку
        from IPython.display import Javascript, display as jsdisplay
        jsdisplay(Javascript('''
        Array.from(document.querySelectorAll('.widget-box, .widget-hbox, .widget-vbox, .widget-container'))
          .forEach(el => { el.style.overflowX = 'hidden'; el.style.maxWidth = '100vw'; });
        '''))

    def _on_export_local_data_clicked(self, button, export_format: str) -> None:
        """
        Экспорт выбранных данных в указанный формат (CSV/Parquet) из правой колонки.
        """
        with self.output:
            clear_output(wait=True)
            selected_items = [(symbol, timeframe) for (symbol, timeframe), cb in self.local_data_checkboxes.items() if cb.value]
            if not selected_items:
                print("Ничего не выбрано для экспорта.")
                return
            exports_dir = os.path.join(self.db_manager.db_directory, 'exports')
            os.makedirs(exports_dir, exist_ok=True)
            for symbol, timeframe in selected_items:
                row = self.current_stored_info[(self.current_stored_info['symbol'] == symbol) & (self.current_stored_info['timeframe'] == timeframe)].iloc[0]
                start_date_obj = pd.to_datetime(row['start_date'])
                end_date_obj = pd.to_datetime(row['end_date'])
                df = self.db_manager.get_data(symbol, timeframe, start_date_obj, end_date_obj)
                if df is not None and not df.empty:
                    filename = f"{symbol}_{timeframe}_{start_date_obj.strftime('%Y%m%d')}_{end_date_obj.strftime('%Y%m%d')}.{export_format.lower()}"
                    filepath = os.path.join(exports_dir, filename)
                    try:
                        if export_format == 'CSV':
                            df.to_csv(filepath, index=True)
                        elif export_format == 'Parquet':
                            df.to_parquet(filepath, index=True)
                        print(f"Экспортировано: {filepath}")
                    except Exception as e:
                        print(f"Ошибка экспорта {symbol} - {timeframe}: {e}")
                else:
                    print(f"Нет данных для {symbol} - {timeframe}.")

    def _on_delete_local_selected_clicked(self, button) -> None:
        with self.output:
            clear_output(wait=True)
            print("Удаление через этот способ больше не поддерживается. Используйте удаление через правый блок.")

    def _on_delete_local_selected_from_list_clicked(self, button) -> None:
        """
        Удаление выбранных данных из базы данных (правый список, с подтверждением).
        """
        with self.output:
            clear_output(wait=True)
            selected_items = [(symbol, timeframe) for (symbol, timeframe), cb in self.local_data_checkboxes.items() if cb.value]
            if not selected_items:
                print("Ничего не выбрано для удаления.")
                return
            if not self.confirm_delete_local_list_checkbox.value:
                print("Пожалуйста, подтвердите удаление (отметьте чекбокс).")
                return
            for symbol, timeframe in selected_items:
                try:
                    self.db_manager.delete_data(symbol, timeframe)
                    print(f"Удалено: {symbol} - {timeframe}")
                except Exception as e:
                    print(f"Ошибка удаления {symbol} - {timeframe}: {e}")
            self.confirm_delete_local_list_checkbox.value = False
            # После удаления обновить список
            self._on_show_local_button_clicked(None)

    def _on_load_as_current_df_clicked(self, button) -> None:
        with self.output:
            clear_output(wait=True)
            selected_items = [(symbol, timeframe) for (symbol, timeframe), cb in self.local_data_checkboxes.items() if cb.value]
            if not selected_items:
                print("Ничего не выбрано для загрузки.")
                return
            if len(selected_items) > 1:
                print("Пожалуйста, выберите только один инструмент для загрузки как текущий датафрейм.")
                return
            symbol, timeframe = selected_items[0]
            row = self.current_stored_info[(self.current_stored_info['symbol'] == symbol) & (self.current_stored_info['timeframe'] == timeframe)].iloc[0]
            start_date_obj = pd.to_datetime(row['start_date'])
            end_date_obj = pd.to_datetime(row['end_date'])
            df = self.db_manager.get_data(symbol, timeframe, start_date_obj, end_date_obj)
            if df is not None and not df.empty:
                globals()['selected_df'] = df
                print(f"Загружено в переменную selected_df: {symbol} {timeframe} ({len(df)} строк)")
                print("Первые 5 строк:")
                display(df.head())
                print("Последние 5 строк:")
                display(df.tail())
            else:
                print(f"Нет данных для {symbol} - {timeframe}.")