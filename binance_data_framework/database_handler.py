#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для работы с локальной базой данных для хранения данных Binance.
"""

import os
import pandas as pd
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Union

class LocalDataManager:
    """
    Класс для управления локальной базой данных для хранения исторических данных.
    Предназначен для работы исключительно в среде Google Colab с Google Drive.
    """

    def __init__(self):
        """
        Инициализация менеджера локальной базы данных.
        Автоматически проверяет среду выполнения и настраивает путь к базе данных на Google Drive.
        В случае запуска вне Colab выбрасывает исключение.
        """
        self.conn = None
        self.cursor = None
        
        try:
            # Проверяем, запущен ли код в Colab
            from IPython import get_ipython
            is_colab = 'google.colab' in str(get_ipython())
        except:
            is_colab = False
            
        if not is_colab:
            raise RuntimeError("Ошибка: Фреймворк binance_data_framework предназначен для использования только в среде Google Colab.")
            
        # Если мы здесь, значит код выполняется в Colab
        print("Обнаружена среда Google Colab.")
        try:
            # Импортируем необходимые модули для работы с Google Drive
            import os
            from google.colab import drive

            drive_mount_point = '/content/drive'
            if not os.path.ismount(drive_mount_point):
                print(f"Google Drive не смонтирован. Попытка монтирования в {drive_mount_point}...")
                try:
                    drive.mount(drive_mount_point, force_remount=True)
                    print("Google Drive успешно смонтирован.")
                except Exception as e:
                    raise RuntimeError(f"Не удалось смонтировать Google Drive: {e}. Автоматическое использование БД на Google Drive невозможно.")
            else:
                print("Google Drive уже смонтирован.")

            # Фиксированный путь к базе данных
            db_directory = '/content/drive/MyDrive/database'
            fixed_db_path = os.path.join(db_directory, 'binance_ohlcv_data.db')
            
            # Создаем директорию для БД, если она не существует
            try:
                os.makedirs(db_directory, exist_ok=True)
                print(f"Директория '{db_directory}' проверена/создана.")
            except OSError as e:
                raise RuntimeError(f"Не удалось создать директорию для БД: {e}")
                
            # Устанавливаем путь к БД
            self.db_path = fixed_db_path
            print(f"Путь к БД: {self.db_path}")

        except ImportError as e:
            raise RuntimeError(f"Ошибка: Не удалось импортировать необходимые модули для работы с Google Colab: {e}") 
        except Exception as e:
            raise RuntimeError(f"Непредвиденная ошибка при настройке для Colab: {e}")

        # Подключаемся к БД и инициализируем таблицы
        if self._connect():
            self.initialize_db()
        else:
            # Если подключение не удалось, бросаем исключение
            raise ConnectionError(f"Не удалось подключиться к базе данных по пути: {self.db_path}")

    def _connect(self) -> bool:
        """
        Устанавливает соединение с базой данных. Использует self.db_path.

        Returns:
            bool: True, если соединение успешно, иначе False
        """
        try:
            # Убедимся, что путь к БД установлен
            if not hasattr(self, 'db_path') or not self.db_path:
                 print("Ошибка: Путь к базе данных не определен перед подключением.")
                 return False
            print(f"Попытка подключения к БД: {self.db_path}")
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print("Соединение с БД установлено успешно.")
            return True
        except sqlite3.Error as e:
            print(f"Ошибка подключения к базе данных SQLite: {e}")
            self.conn = None # Сбрасываем соединение в случае ошибки
            self.cursor = None
            return False
        except Exception as e:
            print(f"Непредвиденная ошибка при подключении к базе данных: {e}")
            self.conn = None
            self.cursor = None
            return False

    def initialize_db(self) -> None:
        """
        Создает таблицы в базе данных, если их еще нет.
        """
        try:
            if not self.conn:
                self._connect()
            
            # Создаем таблицу для хранения OHLCV данных
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS ohlcv_data (
                    timestamp INTEGER, 
                    symbol TEXT, 
                    timeframe TEXT, 
                    open REAL, 
                    high REAL, 
                    low REAL, 
                    close REAL, 
                    volume REAL,
                    PRIMARY KEY (timestamp, symbol, timeframe)
                )
            ''')
            
            # Создаем индексы для ускорения запросов
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON ohlcv_data (symbol)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_timeframe ON ohlcv_data (timeframe)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON ohlcv_data (timestamp)')
            
            # Создаем таблицу метаданных для отслеживания загруженных данных
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    symbol TEXT,
                    timeframe TEXT,
                    start_date INTEGER,
                    end_date INTEGER,
                    last_update INTEGER,
                    PRIMARY KEY (symbol, timeframe)
                )
            ''')
            
            self.conn.commit()
            print("База данных инициализирована")
        except sqlite3.Error as e:
            print(f"Ошибка при инициализации базы данных: {e}")
        except Exception as e:
            print(f"Непредвиденная ошибка при инициализации базы данных: {e}")
    
    def _timestamp_to_ms(self, dt: datetime) -> int:
        """
        Преобразует объект datetime в миллисекунды.
        
        Args:
            dt: Объект datetime
            
        Returns:
            int: Timestamp в миллисекундах
        """
        return int(dt.timestamp() * 1000)
    
    def _ms_to_datetime(self, ms: int) -> datetime:
        """
        Преобразует миллисекунды в объект datetime.
        
        Args:
            ms: Timestamp в миллисекундах
            
        Returns:
            datetime: Объект datetime
        """
        return datetime.fromtimestamp(ms / 1000)
    
    def save_data(self, df: pd.DataFrame, symbol: str, timeframe: str) -> bool:
        """
        Сохраняет данные из DataFrame в базу данных.
        
        Args:
            df: DataFrame с OHLCV данными
            symbol: Торговая пара
            timeframe: Таймфрейм
            
        Returns:
            bool: True, если данные успешно сохранены, иначе False
        """
        try:
            if df.empty:
                print("Пустой DataFrame, нечего сохранять")
                return False
            
            if not self.conn:
                self._connect()
            
            # Подготавливаем данные для вставки
            # Мы ожидаем, что индексом является timestamp в datetime
            df_copy = df.copy()
            
            # Если индекс не является datetime, преобразуем его
            if not isinstance(df_copy.index, pd.DatetimeIndex):
                print("Индекс не является DatetimeIndex, пытаемся преобразовать")
                df_copy.index = pd.to_datetime(df_copy.index)
            
            # Добавляем колонки symbol и timeframe
            df_copy['symbol'] = symbol
            df_copy['timeframe'] = timeframe
            
            # Преобразуем индекс в timestamp в миллисекундах
            df_copy['timestamp'] = df_copy.index.astype(int) // 10**6  # наносекунды в миллисекунды
            
            # Сбрасываем индекс для подготовки данных к вставке
            df_copy.reset_index(drop=True, inplace=True)
            
            # Выбираем нужные колонки в нужном порядке
            columns = ['timestamp', 'symbol', 'timeframe', 'open', 'high', 'low', 'close', 'volume']
            df_prepared = df_copy[columns]
            
            # Вставляем данные
            df_prepared.to_sql('ohlcv_data', self.conn, if_exists='append', index=False, method='multi')
            
            # Обновляем метаданные
            min_timestamp = df_copy['timestamp'].min()
            max_timestamp = df_copy['timestamp'].max()
            current_time = int(datetime.now().timestamp() * 1000)
            
            # Проверяем, существуют ли уже метаданные для этого символа и таймфрейма
            self.cursor.execute(
                'SELECT start_date, end_date FROM metadata WHERE symbol = ? AND timeframe = ?',
                (symbol, timeframe)
            )
            result = self.cursor.fetchone()
            
            if result:
                # Обновляем существующие метаданные
                existing_start, existing_end = result
                start_date = min(existing_start, min_timestamp)
                end_date = max(existing_end, max_timestamp)
                
                self.cursor.execute(
                    'UPDATE metadata SET start_date = ?, end_date = ?, last_update = ? WHERE symbol = ? AND timeframe = ?',
                    (start_date, end_date, current_time, symbol, timeframe)
                )
            else:
                # Вставляем новые метаданные
                self.cursor.execute(
                    'INSERT INTO metadata (symbol, timeframe, start_date, end_date, last_update) VALUES (?, ?, ?, ?, ?)',
                    (symbol, timeframe, min_timestamp, max_timestamp, current_time)
                )
            
            self.conn.commit()
            print(f"Данные успешно сохранены для {symbol} на таймфрейме {timeframe}")
            return True
            
        except sqlite3.IntegrityError:
            # Ошибка уникальности (дубликаты)
            print("Произошла ошибка уникальности при сохранении данных. Некоторые данные уже существуют.")
            self.conn.rollback()
            
            # Альтернативный подход - вставка с игнорированием дубликатов
            try:
                # Преобразуем DataFrame в список кортежей
                data_to_insert = df_copy[columns].values.tolist()
                
                # Вставляем данные с игнорированием дубликатов
                self.cursor.executemany(
                    'INSERT OR IGNORE INTO ohlcv_data (timestamp, symbol, timeframe, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    data_to_insert
                )
                
                # Обновляем метаданные как и раньше
                self.conn.commit()
                print(f"Данные успешно сохранены (исключая дубликаты) для {symbol} на таймфрейме {timeframe}")
                return True
                
            except Exception as e:
                print(f"Ошибка при альтернативном сохранении данных: {e}")
                self.conn.rollback()
                return False
                
        except Exception as e:
            print(f"Ошибка при сохранении данных: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def check_data_exists(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Tuple[bool, Optional[Tuple[datetime, datetime]]]:
        """
        Проверяет наличие данных для указанного символа, таймфрейма и периода.
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
            
        Returns:
            Tuple[bool, Optional[Tuple[datetime, datetime]]]: 
                - bool: True, если данные существуют, иначе False
                - Optional[Tuple[datetime, datetime]]: Доступный диапазон дат, если данные существуют
        """
        try:
            if not self.conn:
                self._connect()
            
            # Диагностическое логирование
            print(f"\n[DEBUG check_data_exists] Проверка: symbol={symbol}, timeframe={timeframe}")
            print(f"[DEBUG check_data_exists] Запрошенный период: start={start_date}, end={end_date}")
            
            # Преобразуем даты в миллисекунды
            start_ms = self._timestamp_to_ms(start_date)
            end_ms = self._timestamp_to_ms(end_date)
            print(f"[DEBUG check_data_exists] Запрошенный период (ms): start_ms={start_ms}, end_ms={end_ms}")
            
            # Сначала проверяем метаданные для быстрого ответа
            self.cursor.execute(
                'SELECT start_date, end_date FROM metadata WHERE symbol = ? AND timeframe = ?',
                (symbol, timeframe)
            )
            result = self.cursor.fetchone()
            
            if not result:
                print(f"[DEBUG check_data_exists] Метаданные для {symbol}/{timeframe} не найдены.")
                print("[DEBUG check_data_exists] Возврат: False (нет метаданных)")
                # Нет метаданных, значит нет данных
                return False, None
            
            meta_start, meta_end = result
            print(f"[DEBUG check_data_exists] Найдены метаданные (ms): meta_start={meta_start}, meta_end={meta_end}")
            
            # Проверяем, покрывают ли имеющиеся данные запрошенный период
            covers_full_period_meta = (meta_start <= start_ms and meta_end >= end_ms)
            print(f"[DEBUG check_data_exists] Покрытие по метаданным (covers_full_period): {covers_full_period_meta}")
            
            if covers_full_period_meta:
                print("[DEBUG check_data_exists] Возврат: True (по метаданным)")
                # Данные полностью покрывают запрошенный период
                return True, (self._ms_to_datetime(meta_start), self._ms_to_datetime(meta_end))
            else:
                print("[DEBUG check_data_exists] Период НЕ покрыт по метаданным.")
            
            # Проверяем фактическое наличие данных в базе
            self.cursor.execute(
                '''
                SELECT MIN(timestamp), MAX(timestamp) 
                FROM ohlcv_data 
                WHERE symbol = ? AND timeframe = ? AND timestamp >= ? AND timestamp <= ?
                ''',
                (symbol, timeframe, start_ms, end_ms)
            )
            result = self.cursor.fetchone()
            
            if not result or result[0] is None or result[1] is None:
                print(f"[DEBUG check_data_exists] Фактические данные для запрошенного периода не найдены.")
                print("[DEBUG check_data_exists] Возврат: False (нет фактических данных)")
                # Нет данных для указанного периода
                return False, None
            
            actual_start, actual_end = result
            print(f"[DEBUG check_data_exists] Найдены фактические данные (ms): actual_start={actual_start}, actual_end={actual_end}")
            
            # Проверяем, покрывают ли фактические данные весь запрошенный период
            covers_full_period_actual = (actual_start <= start_ms and actual_end >= end_ms)
            print(f"[DEBUG check_data_exists] Покрытие по факт. данным (covers_full_period): {covers_full_period_actual}")
            
            if covers_full_period_actual:
                # Данные полностью покрывают запрошенный период
                print(f"[DEBUG check_data_exists] Возврат: True (по факт. данным)")
                return True, (self._ms_to_datetime(actual_start), self._ms_to_datetime(actual_end))
            
            # Данные частично покрывают период
            print(f"[DEBUG check_data_exists] Данные лишь частично покрывают запрошенный период.")
            print(f"[DEBUG check_data_exists] Возврат: False (частичное покрытие)")
            return False, (self._ms_to_datetime(meta_start), self._ms_to_datetime(meta_end))
            
        except sqlite3.Error as e:
            print(f"Ошибка при проверке наличия данных: {e}")
            return False, None
        except Exception as e:
            print(f"Непредвиденная ошибка при проверке наличия данных: {e}")
            return False, None

    def get_data(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Получает данные из базы данных для указанного символа, таймфрейма и периода.
        
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
            
        Returns:
            pd.DataFrame: DataFrame с OHLCV данными
        """
        try:
            if not self.conn:
                self._connect()
            
            # Преобразуем даты в миллисекунды
            start_ms = self._timestamp_to_ms(start_date)
            end_ms = self._timestamp_to_ms(end_date)
            
            # Запрашиваем данные
            query = '''
                SELECT timestamp, open, high, low, close, volume
                FROM ohlcv_data
                WHERE symbol = ? AND timeframe = ? AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp
            '''
            
            df = pd.read_sql_query(
                query, 
                self.conn, 
                params=(symbol, timeframe, start_ms, end_ms)
            )
            
            if df.empty:
                print(f"Данные не найдены для {symbol} на таймфрейме {timeframe} в указанном периоде")
                return pd.DataFrame()
            
            # Преобразуем timestamp в datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Устанавливаем timestamp в качестве индекса
            df.set_index('timestamp', inplace=True)
            
            print(f"Получено {len(df)} свечей для {symbol} на таймфрейме {timeframe}")
            
            return df
            
        except sqlite3.Error as e:
            print(f"Ошибка при получении данных из БД: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Непредвиденная ошибка при получении данных из БД: {e}")
            return pd.DataFrame()
    
    def get_stored_info(self) -> pd.DataFrame:
        """
        Получает информацию о всех сохраненных данных в базе.
        
        Returns:
            pd.DataFrame: DataFrame с информацией о сохраненных данных
        """
        try:
            if not self.conn:
                self._connect()
            
            # Запрашиваем метаданные из БД
            query = '''
                SELECT symbol, timeframe, start_date, end_date, last_update
                FROM metadata
                ORDER BY symbol, timeframe
            '''
            
            df = pd.read_sql_query(query, self.conn)
            
            if df.empty:
                print("В базе данных нет сохраненных данных")
                return pd.DataFrame()
            
            # Преобразуем timestamp в datetime
            timestamp_columns = ['start_date', 'end_date', 'last_update']
            for col in timestamp_columns:
                df[col] = pd.to_datetime(df[col], unit='ms')
            
            return df
            
        except sqlite3.Error as e:
            print(f"Ошибка при получении информации о сохраненных данных: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Непредвиденная ошибка при получении информации о сохраненных данных: {e}")
            return pd.DataFrame()
    
    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None