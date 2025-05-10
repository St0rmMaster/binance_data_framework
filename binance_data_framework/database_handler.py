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
    Предназначен для работы исключительно в среде Google Colab с Google Drive.
    """

    def __init__(self):
        """
        Инициализация менеджера базы данных на Google Drive.
        Автоматически проверяет среду выполнения и настраивает путь к базе данных на Google Drive.
        В случае запуска вне Colab выбрасывает исключение.
        """
        # Попытка импорта get_ipython для определения среды
        try:
            from IPython import get_ipython
            is_colab = 'google.colab' in str(get_ipython())
        except ImportError:
            is_colab = False # Если IPython не установлен, точно не Colab

        if not is_colab:
            raise RuntimeError("ОШИБКА: Фреймворк предназначен для использования ИСКЛЮЧИТЕЛЬНО в среде Google Colab.")

        print("Обнаружена среда Google Colab. Используется Google Drive для хранения БД.")

        from google.colab import drive
        drive_mount_point = '/content/drive'
        if not os.path.ismount(drive_mount_point):
            print(f"Google Drive не смонтирован. Попытка монтирования в {drive_mount_point}...")
            try:
                drive.mount(drive_mount_point, force_remount=True)
                print("Google Drive успешно смонтирован.")
            except Exception as e:
                raise RuntimeError(f"ОШИБКА: Не удалось смонтировать Google Drive: {e}. Работа фреймворка невозможна.")
        else:
            print("Google Drive уже смонтирован.")

        db_parent_directory = '/content/drive/MyDrive/' # Основная папка MyDrive
        self.db_directory = os.path.join(db_parent_directory, 'database_binance_framework') # Имя папки для БД
        db_filename = 'binance_ohlcv_data.db'
        self.db_path = os.path.join(self.db_directory, db_filename)

        try:
            os.makedirs(self.db_directory, exist_ok=True)
            print(f"Директория для БД '{self.db_directory}' проверена/создана.")
        except OSError as e:
            raise RuntimeError(f"ОШИБКА: Не удалось создать директорию для БД '{self.db_directory}': {e}")

        print(f"Путь к БД на Google Drive: {self.db_path}")

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
            self.conn = None # Сбрасываем соединение в случае ошибки
            self.cursor = None
            return False
        except Exception as e:
            print(f"Непредвиденная ошибка при подключении к БД на Google Drive: {e}")
            self.conn = None
            self.cursor = None
            return False

    def initialize_db(self) -> None:
        """
        Создает таблицы в базе данных, если их еще нет.
        """
        try:
            if not self.conn:
                print("Нет соединения с БД на Google Drive для инициализации.")
                return
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
        Сохраняет данные из DataFrame в базу данных на Google Drive.
        Args:
            df: DataFrame с OHLCV данными
            symbol: Торговая пара
            timeframe: Таймфрейм
        Returns:
            bool: True, если данные успешно сохранены в БД на Google Drive, иначе False
        """
        try:
            if df is None or df.empty:
                print("Нет данных для сохранения в БД на Google Drive.")
                return False
            # Создаем копию DataFrame, сбрасываем индекс, чтобы timestamp стал колонкой
            df_to_save = df.copy().reset_index()
            # Преобразуем timestamp (datetime) в миллисекунды
            df_to_save['timestamp'] = df_to_save['timestamp'].apply(self._timestamp_to_ms)
            # Добавляем колонки symbol и timeframe
            df_to_save['symbol'] = symbol
            df_to_save['timeframe'] = timeframe
            # Устанавливаем порядок колонок
            columns_order = ['timestamp', 'symbol', 'timeframe', 'open', 'high', 'low', 'close', 'volume']
            df_to_save = df_to_save[columns_order]
            # Получаем записи для вставки
            records = df_to_save.to_records(index=False)
            self.cursor.executemany(
                'INSERT OR REPLACE INTO ohlcv_data (timestamp, symbol, timeframe, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                records
            )
            # Обновляем метаданные
            start_ts = int(df_to_save['timestamp'].min())
            end_ts = int(df_to_save['timestamp'].max())
            self.cursor.execute(
                'INSERT OR REPLACE INTO ohlcv_metadata (symbol, timeframe, start_timestamp, end_timestamp) VALUES (?, ?, ?, ?)',
                (symbol, timeframe, start_ts, end_ts)
            )
            self.conn.commit()
            print(f"Данные успешно сохранены в БД на Google Drive для {symbol}/{timeframe}.")
            return True
        except sqlite3.IntegrityError:
            print("Ошибка целостности при сохранении данных в БД на Google Drive.")
            return False
        except Exception as e:
            print(f"Ошибка при сохранении данных в БД на Google Drive: {e}")
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
        print(f"\n[DEBUG check_data_exists] Проверка: symbol={symbol}, timeframe={timeframe}")
        print(f"[DEBUG check_data_exists] Запрошенный период: start={start_date}, end={end_date}")
        start_ms = self._timestamp_to_ms(start_date)
        end_ms = self._timestamp_to_ms(end_date)
        print(f"[DEBUG check_data_exists] Запрошенный период (ms): start_ms={start_ms}, end_ms={end_ms}")
        try:
            self.cursor.execute(
                'SELECT start_timestamp, end_timestamp FROM ohlcv_metadata WHERE symbol=? AND timeframe=?',
                (symbol, timeframe)
            )
            result = self.cursor.fetchone()
            if result:
                meta_start_db, meta_end_db = result
                print(f"[DEBUG check_data_exists] Найдены метаданные (ms): meta_start_db={meta_start_db}, meta_end_db={meta_end_db}")
                covers_full_period_meta = (meta_start_db <= start_ms and meta_end_db >= end_ms)
                print(f"[DEBUG check_data_exists] Покрытие по метаданным (covers_full_period_meta): {covers_full_period_meta}")
                if covers_full_period_meta:
                    print(f"[DEBUG check_data_exists] Возврат: True (по метаданным), диапазон: ({self._ms_to_datetime(meta_start_db)}, {self._ms_to_datetime(meta_end_db)})")
                    return True, (self._ms_to_datetime(meta_start_db), self._ms_to_datetime(meta_end_db))
                # Можно добавить дополнительную проверку по фактическим данным, если нужно
            else:
                print(f"[DEBUG check_data_exists] Метаданные для {symbol}/{timeframe} не найдены.")
                print("[DEBUG check_data_exists] Возврат: False, None (нет метаданных)")
                return False, None
            print("[DEBUG check_data_exists] Возврат: False, None (не покрывает период)")
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
                print("Нет данных в БД на Google Drive для указанного периода.")
                return pd.DataFrame()
            columns = ['timestamp', 'symbol', 'timeframe', 'open', 'high', 'low', 'close', 'volume']
            df = pd.DataFrame(rows, columns=columns)
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