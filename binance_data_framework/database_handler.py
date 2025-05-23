#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для работы с базой данных на Google Drive для хранения данных Binance.
"""
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Union
import os
import sqlite3
import pandas as pd

class GoogleDriveDataManager:
    """
    Класс для управления базой данных на Google Drive для хранения исторических данных.
    Может работать как в среде Google Colab, так и в других средах (локально).
    """

    def __init__(self):
        """
        Инициализация менеджера базы данных на Google Drive.
        Автоматически проверяет среду выполнения и настраивает путь к базе данных на Google Drive (если Colab).
        В других средах использует локальный путь.
        """
        try:
            from IPython import get_ipython
            is_colab = 'google.colab' in str(get_ipython())
        except ImportError:
            is_colab = False
        except NameError:
            is_colab = False

        if is_colab:
            try:
                from google.colab import drive
                drive_mount_point = '/content/drive'
                if not os.path.ismount(drive_mount_point):
                    try:
                        drive.mount(drive_mount_point, force_remount=True)
                        print("Google Drive успешно смонтирован.")
                    except Exception as e:
                        print(f"ОШИБКА: Не удалось смонтировать Google Drive: {e}. Работа фреймворка невозможна.")
                else:
                    print("Google Drive уже смонтирован.")
                db_parent_directory = '/content/drive/MyDrive/'
            except Exception:
                db_parent_directory = os.path.abspath('.')
        else:
            db_parent_directory = os.path.abspath('.')

        self.db_directory = os.path.join(db_parent_directory, 'database_binance_framework')
        db_filename = 'binance_ohlcv_data.db'
        self.db_path = os.path.join(self.db_directory, db_filename)

        try:
            os.makedirs(self.db_directory, exist_ok=True)
        except OSError as e:
            raise RuntimeError(f"ОШИБКА: Не удалось создать директорию для БД '{self.db_directory}': {e}")

        self.conn = None
        self.cursor = None
        if self._connect():
            self.initialize_db()
        else:
            raise RuntimeError(f"ОШИБКА: Не удалось подключиться к БД на Google Drive: {self.db_path}")

    def _connect(self) -> bool:
        """
        Устанавливает соединение с базой данных. Использует self.db_path.

        Returns:
            bool: True, если соединение успешно, иначе False
        """
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.cursor = self.conn.cursor()
            return True
        except sqlite3.Error as e:
            print(f"Ошибка подключения к БД на Google Drive ({self.db_path}): {e}")
            self.conn = None
            self.cursor = None
            return False
        except Exception as e:
            print(f"Непредвиденная ошибка при подключении к БД на Google Drive: {e}")
            self.conn = None
            self.cursor = None
            return False

    def initialize_db(self) -> None:
        """
        Создает таблицы в базе данных, если их еще нет. Если структура некорректна (timestamp не INTEGER), пересоздает таблицы.
        """
        try:
            if not self.conn:
                print("Нет соединения с БД на Google Drive для инициализации.")
                return
            # Проверка структуры таблицы ohlcv_data
            self.cursor.execute("PRAGMA table_info(ohlcv_data);")
            columns = self.cursor.fetchall()
            needs_recreate = False
            for col in columns:
                if (col[1] == 'timestamp' and col[2].upper() != 'INTEGER'):
                    print("❌ Обнаружен некорректный тип timestamp в ohlcv_data. Будет выполнено пересоздание таблицы.")
                    needs_recreate = True
            if needs_recreate:
                self.cursor.execute("DROP TABLE IF EXISTS ohlcv_data;")
                self.cursor.execute("DROP TABLE IF EXISTS ohlcv_metadata;")
                self.conn.commit()
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
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON ohlcv_data (symbol)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_timeframe ON ohlcv_data (timeframe)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON ohlcv_data (timestamp)')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS ohlcv_metadata (
                    symbol TEXT,
                    timeframe TEXT,
                    start_timestamp INTEGER,
                    end_timestamp INTEGER,
                    PRIMARY KEY (symbol, timeframe)
                )
            ''')
            self.conn.commit()
            print(f"База данных на Google Drive ({self.db_path}) инициализирована.")
        except sqlite3.Error as e:
            print(f"Ошибка инициализации БД на Google Drive: {e}")
        except Exception as e:
            print(f"Непредвиденная ошибка при инициализации БД на Google Drive: {e}")

    def _timestamp_to_ms(self, x):
        """
        Преобразует timestamp (int, float, datetime, pd.Timestamp) в миллисекунды.
        """
        if isinstance(x, (int, float)):
            return int(x)
        if hasattr(x, 'timestamp'):
            return int(x.timestamp() * 1000)
        raise ValueError(f"Неизвестный формат timestamp: {x}")

    def _ms_to_datetime(self, ms: int) -> datetime:
        """
        Преобразует миллисекунды в объект datetime.
        Args:
            ms: Timestamp в миллисекундах
        Returns:
            datetime: Объект datetime
        """
        return datetime.fromtimestamp(ms / 1000)

    def _get_timeframe_duration_ms(self, timeframe: str) -> Optional[int]:
        """
        Возвращает длительность таймфрейма в миллисекундах.
        Args:
            timeframe: строка таймфрейма Binance (например, '1m', '5m', '1h', '1d', '1w', '1M')
        Returns:
            int: длительность таймфрейма в миллисекундах, либо None если не удалось определить
        """
        mapping = {
            '1m': 60 * 1000,
            '3m': 3 * 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '30m': 30 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '6h': 6 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '12h': 12 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000,
            '3d': 3 * 24 * 60 * 60 * 1000,
            '1w': 7 * 24 * 60 * 60 * 1000,
        }
        if timeframe in mapping:
            return mapping[timeframe]
        elif timeframe == '1M':
            return 30 * 24 * 60 * 60 * 1000
        else:
            return None

    def save_data(self, df: pd.DataFrame, symbol: str, timeframe: str) -> bool:
        """
        Сохраняет данные из DataFrame в базу данных на Google Drive.
        Args:
            df: DataFrame с OHLCV данными
            symbol: Торговая пара
            timeframe: Таймфрейм
        Returns:
            bool: True, если данные успешно сохранены в БД на Google Drive, иначе False
        """
        try:
            if not symbol or not isinstance(symbol, str) or symbol.strip() == '':
                raise ValueError("Symbol не должен быть пустым!")
            if df is None or df.empty:
                return False
            df_to_save = df.copy().reset_index()
            df_to_save['timestamp'] = df_to_save['timestamp'].apply(lambda x: int(self._timestamp_to_ms(x)))
            df_to_save['symbol'] = symbol
            df_to_save['timeframe'] = timeframe
            columns_order = ['timestamp', 'symbol', 'timeframe', 'open', 'high', 'low', 'close', 'volume']
            df_to_save = df_to_save[columns_order]
            records = df_to_save.values.tolist()
            self.cursor.executemany(
                'INSERT OR REPLACE INTO ohlcv_data (timestamp, symbol, timeframe, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                records
            )
            self.cursor.execute(
                'SELECT MIN(timestamp), MAX(timestamp) FROM ohlcv_data WHERE symbol=? AND timeframe=?',
                (symbol, timeframe)
            )
            row = self.cursor.fetchone()
            if row and row[0] is not None and row[1] is not None:
                min_ts, max_ts = int(row[0]), int(row[1])
                self.cursor.execute(
                    'INSERT OR REPLACE INTO ohlcv_metadata (symbol, timeframe, start_timestamp, end_timestamp) VALUES (?, ?, ?, ?)',
                    (symbol, timeframe, min_ts, max_ts)
                )
            self.conn.commit()
            print(f"Данные успешно сохранены в БД на Google Drive для {symbol}/{timeframe}.")
            return True
        except ValueError as e:
            print(f"Ошибка при сохранении данных в БД на Google Drive: {e}")
            raise
        except sqlite3.IntegrityError:
            print("Ошибка целостности при сохранении данных в БД на Google Drive.")
            return False
        except Exception as e:
            print(f"Ошибка при сохранении данных в БД на Google Drive: {e}")
            return False

    def delete_data(self, symbol: str, timeframe: str) -> bool:
        """
        Удаляет все данные и метаданные для указанного symbol и timeframe.
        """
        try:
            self.cursor.execute('DELETE FROM ohlcv_data WHERE symbol=? AND timeframe=?', (symbol, timeframe))
            self.cursor.execute('DELETE FROM ohlcv_metadata WHERE symbol=? AND timeframe=?', (symbol, timeframe))
            self.conn.commit()
            print(f"Данные для {symbol}/{timeframe} успешно удалены.")
            return True
        except Exception as e:
            print(f"Ошибка при удалении данных для {symbol}/{timeframe}: {e}")
            return False

    def check_data_exists(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Tuple[bool, Optional[Tuple[datetime, datetime]]]:
        """
        Проверяет наличие данных для указанного символа, таймфрейма и периода в БД на Google Drive.
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
        Returns:
            Tuple[bool, Optional[Tuple[datetime, datetime]]]: 
                - bool: True, если данные существуют в БД на Google Drive, иначе False
                - Optional[Tuple[datetime, datetime]]: Доступный диапазон дат, если данные существуют
        """
        start_ms = self._timestamp_to_ms(start_date)
        end_ms = self._timestamp_to_ms(end_date)
        try:
            self.cursor.execute(
                'SELECT start_timestamp, end_timestamp FROM ohlcv_metadata WHERE symbol=? AND timeframe=?',
                (symbol, timeframe)
            )
            result = self.cursor.fetchone()
            if result:
                meta_start_db, meta_end_db = result
                duration_ms = self._get_timeframe_duration_ms(timeframe)
                if duration_ms:
                    actual_coverage_end_ms = meta_end_db + duration_ms - 1
                else:
                    actual_coverage_end_ms = meta_end_db
                import time
                now_ms = int(time.time() * 1000)
                if end_ms > actual_coverage_end_ms:
                    if abs(now_ms - actual_coverage_end_ms) < duration_ms * 2:
                        return True, (self._ms_to_datetime(meta_start_db), self._ms_to_datetime(meta_end_db))
                covers_full_period_meta = (meta_start_db <= start_ms and actual_coverage_end_ms >= end_ms)
                if covers_full_period_meta:
                    return True, (self._ms_to_datetime(meta_start_db), self._ms_to_datetime(meta_end_db))
            else:
                return False, None
            return False, None
        except sqlite3.Error as e:
            print(f"Ошибка при проверке наличия данных в БД на Google Drive: {e}")
            return False, None
        except Exception as e:
            print(f"Непредвиденная ошибка при проверке наличия данных в БД на Google Drive: {e}")
            return False, None

    def get_data(
        self, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Получает данные из базы данных на Google Drive для указанного символа, таймфрейма и периода.
        Args:
            symbol: Торговая пара
            timeframe: Таймфрейм
            start_date: Дата начала периода
            end_date: Дата окончания периода
        Returns:
            pd.DataFrame: DataFrame с OHLCV данными
        """
        try:
            start_ms = self._timestamp_to_ms(start_date)
            end_ms = self._timestamp_to_ms(end_date)
            self.cursor.execute(
                'SELECT * FROM ohlcv_data WHERE symbol=? AND timeframe=? AND timestamp>=? AND timestamp<=? ORDER BY timestamp ASC',
                (symbol, timeframe, start_ms, end_ms)
            )
            rows = self.cursor.fetchall()
            if not rows:
                return pd.DataFrame()
            columns = ['timestamp', 'symbol', 'timeframe', 'open', 'high', 'low', 'close', 'volume']
            df = pd.DataFrame(rows, columns=columns)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            return df
        except sqlite3.Error as e:
            print(f"Ошибка при получении данных из БД на Google Drive: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Непредвиденная ошибка при получении данных из БД на Google Drive: {e}")
            return pd.DataFrame()

    def get_stored_info(self) -> pd.DataFrame:
        """
        Получает информацию о всех сохраненных данных в БД на Google Drive.
        Также проверяет типы timestamp и предупреждает, если есть некорректные типы.
        Returns:
            pd.DataFrame: DataFrame с информацией о сохраненных данных
        """
        try:
            self.cursor.execute('SELECT * FROM ohlcv_metadata')
            rows = self.cursor.fetchall()
            if not rows:
                print("В БД на Google Drive нет сохраненных данных.")
                return pd.DataFrame()
            columns = ['symbol', 'timeframe', 'start_timestamp', 'end_timestamp']
            df = pd.DataFrame(rows, columns=columns)
            df['start_date'] = pd.to_datetime(df['start_timestamp'], unit='ms')
            df['end_date'] = pd.to_datetime(df['end_timestamp'], unit='ms')
            df = df[['symbol', 'timeframe', 'start_date', 'end_date', 'start_timestamp', 'end_timestamp']]
            self.cursor.execute("SELECT DISTINCT typeof(timestamp) FROM ohlcv_data LIMIT 10;")
            types = self.cursor.fetchall()
            if types and any(t[0] != 'integer' for t in types):
                print(f"❌ ВНИМАНИЕ: В таблице ohlcv_data обнаружены некорректные типы timestamp: {types}. Рекомендуется пересоздать таблицу.")
            # print("✅ Все значения timestamp в ohlcv_data имеют тип integer.")
            return df
        except sqlite3.Error as e:
            print(f"Ошибка при получении информации о сохраненных данных в БД на Google Drive: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Непредвиденная ошибка при получении информации о сохраненных данных в БД на Google Drive: {e}")
            return pd.DataFrame()

    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        if self.conn:
            self.conn.close()
            print("Соединение с БД на Google Drive закрыто.")

    def debug_print_ohlcv_data(self, symbol, timeframe, limit=10):
        print(f"Первые {limit} строк для {symbol}/{timeframe}:")
        self.cursor.execute(
            "SELECT timestamp, symbol, timeframe FROM ohlcv_data WHERE symbol=? AND timeframe=? ORDER BY timestamp ASC LIMIT ?",
            (symbol, timeframe, limit)
        )
        rows = self.cursor.fetchall()
        for row in rows:
            ts, sym, tf = row
            print(f"timestamp={ts} ({pd.to_datetime(ts, unit='ms')}), symbol={sym}, timeframe={tf}")

    def debug_check_timestamps(self, symbol: str, timeframe: str, limit: int = 5):
        """
        Проверяет корректность хранения timestamp для заданного symbol/timeframe.
        Выводит типы, значения и результат преобразования в дату.
        """
        print(f"Проверка первых {limit} строк для {symbol}/{timeframe} в ohlcv_data:")
        self.cursor.execute(
            "SELECT timestamp, typeof(timestamp), symbol, timeframe FROM ohlcv_data WHERE symbol=? AND timeframe=? ORDER BY timestamp ASC LIMIT ?",
            (symbol, timeframe, limit)
        )
        rows = self.cursor.fetchall()
        for ts, ttype, sym, tf in rows:
            try:
                dt = pd.to_datetime(ts, unit='ms') if ttype == 'integer' else None
                print(f"timestamp={ts} (type={ttype}) -> {dt}, symbol={sym}, timeframe={tf}")
            except Exception as e:
                print(f"❌ Ошибка преобразования timestamp={ts} (type={ttype}): {e}")
        if not rows:
            print("Нет данных для указанного symbol/timeframe.")